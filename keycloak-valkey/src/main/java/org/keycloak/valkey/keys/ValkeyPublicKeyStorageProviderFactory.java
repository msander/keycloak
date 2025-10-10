package org.keycloak.valkey.keys;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jboss.logging.Logger;
import org.keycloak.Config;
import org.keycloak.cluster.ClusterEvent;
import org.keycloak.cluster.ClusterListener;
import org.keycloak.cluster.ClusterProvider;
import org.keycloak.keys.PublicKeyStorageProvider;
import org.keycloak.keys.PublicKeyStorageProviderFactory;
import org.keycloak.keys.PublicKeyStorageUtils;
import org.keycloak.models.ClientModel;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.models.RealmModel;
import org.keycloak.provider.ProviderConfigProperty;
import org.keycloak.provider.ProviderConfigurationBuilder;
import org.keycloak.provider.ProviderEvent;
import org.keycloak.provider.ProviderEventListener;

/**
 * Factory for the Valkey-backed public key storage provider.
 */
public class ValkeyPublicKeyStorageProviderFactory implements PublicKeyStorageProviderFactory<ValkeyPublicKeyStorageProvider> {

    public static final String PROVIDER_ID = "valkey";
    static final String PUBLIC_KEY_STORAGE_INVALIDATION_EVENT = "valkey-public-key-storage-invalidation";
    static final String KEYS_CLEAR_CACHE_EVENT = "valkey-public-key-clear";

    private static final Logger logger = Logger.getLogger(ValkeyPublicKeyStorageProviderFactory.class);
    private static final ConcurrentMap<String, ValkeyPublicKeysEntry> CACHE = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, CompletableFuture<ValkeyPublicKeysEntry>> TASKS = new ConcurrentHashMap<>();

    private volatile boolean listenersRegistered;
    private int minTimeBetweenRequests;
    private int maxCacheTime;

    @Override
    public ValkeyPublicKeyStorageProvider create(KeycloakSession session) {
        registerClusterListeners(session);
        return new ValkeyPublicKeyStorageProvider(session, CACHE, TASKS, minTimeBetweenRequests, maxCacheTime);
    }

    @Override
    public void init(Config.Scope config) {
        minTimeBetweenRequests = config.getInt("minTimeBetweenRequests", 10);
        maxCacheTime = config.getInt("maxCacheTime", 24 * 60 * 60);
        logger.debugf("minTimeBetweenRequests is %d maxCacheTime is %d", minTimeBetweenRequests, maxCacheTime);
    }

    @Override
    public void postInit(KeycloakSessionFactory factory) {
        factory.register(new ProviderEventListener() {
            @Override
            public void onEvent(ProviderEvent event) {
                SessionAndKeyHolder holder = getCacheKeyToInvalidate(event);
                if (holder == null) {
                    return;
                }
                ValkeyPublicKeyStorageProvider provider = (ValkeyPublicKeyStorageProvider) holder.session
                        .getProvider(PublicKeyStorageProvider.class, getId());
                for (String cacheKey : holder.cacheKeys) {
                    provider.addInvalidation(cacheKey);
                }
            }
        });
    }

    @Override
    public void close() {
        listenersRegistered = false;
    }

    @Override
    public String getId() {
        return PROVIDER_ID;
    }

    @Override
    public List<ProviderConfigProperty> getConfigMetadata() {
        return ProviderConfigurationBuilder.create()
                .property()
                .name("minTimeBetweenRequests")
                .type("int")
                .helpText("Minimum interval in seconds between two requests to retrieve new public keys. "
                        + "Avoids hammering external JWKS endpoints when keys are missing.")
                .defaultValue(10)
                .add()
                .property()
                .name("maxCacheTime")
                .type("int")
                .helpText("Maximum interval in seconds that keys are cached when retrieved via bulk operations. "
                        + "Ensures periodic refresh when no explicit expiration is provided by the protocol.")
                .defaultValue(24 * 60 * 60)
                .add()
                .build();
    }

    static ConcurrentMap<String, ValkeyPublicKeysEntry> cache() {
        return CACHE;
    }

    private void registerClusterListeners(KeycloakSession session) {
        if (listenersRegistered) {
            return;
        }
        synchronized (this) {
            if (listenersRegistered) {
                return;
            }
            ClusterProvider cluster = session.getProvider(ClusterProvider.class);
            if (cluster == null) {
                throw new IllegalStateException("ClusterProvider must be available for Valkey public key storage");
            }
            cluster.registerListener(PUBLIC_KEY_STORAGE_INVALIDATION_EVENT, new ClusterListener() {
                @Override
                public void eventReceived(ClusterEvent event) {
                    ValkeyPublicKeyInvalidationEvent invalidation = (ValkeyPublicKeyInvalidationEvent) event;
                    CACHE.remove(invalidation.getCacheKey());
                }
            });
            cluster.registerListener(KEYS_CLEAR_CACHE_EVENT, new ClusterListener() {
                @Override
                public void eventReceived(ClusterEvent event) {
                    CACHE.clear();
                }
            });
            listenersRegistered = true;
        }
    }

    private SessionAndKeyHolder getCacheKeyToInvalidate(ProviderEvent event) {
        if (event instanceof ClientModel.ClientUpdatedEvent updated) {
            return sessionKeys(updated.getKeycloakSession(), updated.getUpdatedClient().getRealm().getId(),
                    updated.getUpdatedClient().getId());
        } else if (event instanceof ClientModel.ClientRemovedEvent removed) {
            return sessionKeys(removed.getKeycloakSession(), removed.getClient().getRealm().getId(),
                    removed.getClient().getId());
        } else if (event instanceof RealmModel.IdentityProviderUpdatedEvent updatedIdp) {
            List<String> cacheKeys = new ArrayList<>();
            cacheKeys.add(PublicKeyStorageUtils.getIdpModelCacheKey(updatedIdp.getRealm().getId(),
                    updatedIdp.getUpdatedIdentityProvider().getInternalId()));
            return new SessionAndKeyHolder(updatedIdp.getKeycloakSession(), cacheKeys);
        } else if (event instanceof RealmModel.IdentityProviderRemovedEvent removedIdp) {
            List<String> cacheKeys = new ArrayList<>();
            cacheKeys.add(PublicKeyStorageUtils.getIdpModelCacheKey(removedIdp.getRealm().getId(),
                    removedIdp.getRemovedIdentityProvider().getInternalId()));
            return new SessionAndKeyHolder(removedIdp.getKeycloakSession(), cacheKeys);
        }
        return null;
    }

    private SessionAndKeyHolder sessionKeys(KeycloakSession session, String realmId, String clientId) {
        Objects.requireNonNull(session, "session");
        List<String> cacheKeys = new ArrayList<>();
        cacheKeys.add(PublicKeyStorageUtils.getClientModelCacheKey(realmId, clientId, org.keycloak.jose.jwk.JWK.Use.SIG));
        cacheKeys.add(PublicKeyStorageUtils.getClientModelCacheKey(realmId, clientId, org.keycloak.jose.jwk.JWK.Use.ENCRYPTION));
        return new SessionAndKeyHolder(session, cacheKeys);
    }

    private record SessionAndKeyHolder(KeycloakSession session, List<String> cacheKeys) {
    }
}
