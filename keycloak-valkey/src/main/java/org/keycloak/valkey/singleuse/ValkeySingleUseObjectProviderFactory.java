package org.keycloak.valkey.singleuse;

import static org.keycloak.storage.datastore.DefaultDatastoreProviderFactory.setupClearExpiredRevokedTokensScheduledTask;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.keycloak.Config;
import org.keycloak.common.util.Time;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.models.SingleUseObjectProvider;
import org.keycloak.models.SingleUseObjectProviderFactory;
import org.keycloak.models.session.RevokedToken;
import org.keycloak.models.session.RevokedTokenPersisterProvider;
import org.keycloak.provider.Provider;
import org.keycloak.provider.ProviderConfigProperty;
import org.keycloak.provider.ProviderConfigurationBuilder;
import org.keycloak.provider.ServerInfoAwareProviderFactory;
import org.keycloak.valkey.ValkeyConnectionProvider;

import io.lettuce.core.SetArgs;
import io.lettuce.core.api.sync.RedisCommands;
import org.keycloak.util.JsonSerialization;

/**
 * Factory wiring the Valkey-backed {@link SingleUseObjectProvider} implementation.
 */
public class ValkeySingleUseObjectProviderFactory
        implements SingleUseObjectProviderFactory<ValkeySingleUseObjectProvider>, ServerInfoAwareProviderFactory {

    public static final String PROVIDER_ID = "valkey";
    public static final String CONFIG_PERSIST_REVOKED_TOKENS = "persistRevokedTokens";
    public static final boolean DEFAULT_PERSIST_REVOKED_TOKENS = true;
    private static final String LOADED_KEY = "loaded" + SingleUseObjectProvider.REVOKED_KEY;

    private static final String EMPTY_PAYLOAD;

    static {
        EMPTY_PAYLOAD = encodeEmptyPayload();
    }

    private volatile boolean persistRevokedTokens = DEFAULT_PERSIST_REVOKED_TOKENS;
    private final AtomicBoolean revokedTokensLoaded = new AtomicBoolean(false);

    @Override
    public ValkeySingleUseObjectProvider create(KeycloakSession session) {
        ValkeyConnectionProvider connectionProvider = session.getProvider(ValkeyConnectionProvider.class);
        if (connectionProvider == null) {
            throw new IllegalStateException("ValkeyConnectionProvider must be available for Valkey single-use objects");
        }
        if (persistRevokedTokens) {
            ensureRevokedTokensPreloaded(session, connectionProvider);
        }
        return new ValkeySingleUseObjectProvider(session, connectionProvider, persistRevokedTokens);
    }

    @Override
    public void init(Config.Scope config) {
        if (config == null) {
            return;
        }
        this.persistRevokedTokens = config.getBoolean(CONFIG_PERSIST_REVOKED_TOKENS, DEFAULT_PERSIST_REVOKED_TOKENS);
    }

    @Override
    public void postInit(KeycloakSessionFactory factory) {
        if (!persistRevokedTokens) {
            return;
        }
        factory.register(event -> {
            if (event instanceof org.keycloak.models.utils.PostMigrationEvent) {
                setupClearExpiredRevokedTokensScheduledTask(factory);
                try (KeycloakSession session = factory.create()) {
                    ValkeyConnectionProvider connectionProvider = session.getProvider(ValkeyConnectionProvider.class);
                    if (connectionProvider != null) {
                        preloadRevokedTokens(session, connectionProvider.getConnection().sync());
                    }
                }
            }
        });
    }

    @Override
    public void close() {
        revokedTokensLoaded.set(false);
    }

    @Override
    public String getId() {
        return PROVIDER_ID;
    }

    @Override
    public Map<String, String> getOperationalInfo() {
        Map<String, String> info = new HashMap<>();
        info.put(CONFIG_PERSIST_REVOKED_TOKENS, Boolean.toString(persistRevokedTokens));
        return info;
    }

    @Override
    public Set<Class<? extends Provider>> dependsOn() {
        return Set.of(ValkeyConnectionProvider.class);
    }

    @Override
    public List<ProviderConfigProperty> getConfigMetadata() {
        ProviderConfigurationBuilder builder = ProviderConfigurationBuilder.create();
        builder.property()
                .name(CONFIG_PERSIST_REVOKED_TOKENS)
                .type("boolean")
                .helpText("If revoked tokens are stored persistently across restarts")
                .defaultValue(DEFAULT_PERSIST_REVOKED_TOKENS)
                .add();
        return builder.build();
    }

    void ensureRevokedTokensPreloaded(KeycloakSession session, ValkeyConnectionProvider connectionProvider) {
        if (revokedTokensLoaded.get()) {
            return;
        }
        synchronized (this) {
            if (revokedTokensLoaded.get()) {
                return;
            }
            RedisCommands<String, String> commands = connectionProvider.getConnection().sync();
            if (commands.exists(LOADED_KEY) > 0) {
                revokedTokensLoaded.set(true);
                return;
            }
            preloadRevokedTokens(session, commands);
            revokedTokensLoaded.set(true);
        }
    }

    private void preloadRevokedTokens(KeycloakSession session, RedisCommands<String, String> commands) {
        RevokedTokenPersisterProvider provider = session.getProvider(RevokedTokenPersisterProvider.class);
        if (provider == null) {
            return;
        }
        long now = Time.currentTime();
        try (Stream<RevokedToken> stream = provider.getAllRevokedTokens()) {
            stream.forEach(revokedToken -> {
                long lifespanSeconds = revokedToken.expiry() - now;
                if (lifespanSeconds <= 0) {
                    return;
                }
                String key = revokedToken.tokenId() + SingleUseObjectProvider.REVOKED_KEY;
                commands.set(key, EMPTY_PAYLOAD, SetArgs.Builder.nx().ex(lifespanSeconds));
            });
        }
        commands.set(LOADED_KEY, EMPTY_PAYLOAD);
    }

    private static String encodeEmptyPayload() {
        try {
            return JsonSerialization.writeValueAsString(Map.of());
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to encode empty payload for revoked tokens", ex);
        }
    }
}
