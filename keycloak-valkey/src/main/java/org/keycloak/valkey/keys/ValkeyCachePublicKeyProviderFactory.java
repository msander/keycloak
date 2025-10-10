package org.keycloak.valkey.keys;

import java.util.concurrent.ConcurrentMap;

import org.keycloak.Config;
import org.keycloak.cluster.ClusterEvent;
import org.keycloak.cluster.ClusterListener;
import org.keycloak.cluster.ClusterProvider;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.models.cache.CachePublicKeyProvider;
import org.keycloak.models.cache.CachePublicKeyProviderFactory;

/**
 * Factory producing cache accessors for the Valkey public key cache.
 */
public class ValkeyCachePublicKeyProviderFactory implements CachePublicKeyProviderFactory {

    public static final String PROVIDER_ID = "valkey";

    private volatile boolean listenersRegistered;

    @Override
    public CachePublicKeyProvider create(KeycloakSession session) {
        registerClusterListeners(session);
        ConcurrentMap<String, ValkeyPublicKeysEntry> cache = ValkeyPublicKeyStorageProviderFactory.cache();
        return new ValkeyCachePublicKeyProvider(session, cache);
    }

    @Override
    public void init(Config.Scope config) {
    }

    @Override
    public void postInit(KeycloakSessionFactory factory) {
    }

    @Override
    public void close() {
        listenersRegistered = false;
    }

    @Override
    public String getId() {
        return PROVIDER_ID;
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
                throw new IllegalStateException("ClusterProvider must be available for Valkey public key cache");
            }
            cluster.registerListener(ValkeyPublicKeyStorageProviderFactory.KEYS_CLEAR_CACHE_EVENT, new ClusterListener() {
                @Override
                public void eventReceived(ClusterEvent event) {
                    ValkeyPublicKeyStorageProviderFactory.cache().clear();
                }
            });
            cluster.registerListener(ValkeyPublicKeyStorageProviderFactory.PUBLIC_KEY_STORAGE_INVALIDATION_EVENT,
                    new ClusterListener() {
                        @Override
                        public void eventReceived(ClusterEvent event) {
                            ValkeyPublicKeyInvalidationEvent invalidation = (ValkeyPublicKeyInvalidationEvent) event;
                            ValkeyPublicKeyStorageProviderFactory.cache().remove(invalidation.getCacheKey());
                        }
                    });
            listenersRegistered = true;
        }
    }
}
