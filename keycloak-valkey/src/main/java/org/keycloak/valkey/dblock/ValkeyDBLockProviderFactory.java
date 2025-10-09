package org.keycloak.valkey.dblock;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.logging.Logger;
import org.keycloak.Config;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.models.dblock.DBLockProvider;
import org.keycloak.models.dblock.DBLockProviderFactory;
import org.keycloak.provider.ProviderConfigProperty;
import org.keycloak.provider.ProviderConfigurationBuilder;
import org.keycloak.valkey.ValkeyConnectionProvider;
import org.keycloak.valkey.config.ValkeyDBLockConfig;

/**
 * Factory wiring Valkey-backed database locks into Keycloak's DB lock SPI.
 */
public class ValkeyDBLockProviderFactory implements DBLockProviderFactory {

    public static final String PROVIDER_ID = "valkey";

    private static final Logger logger = Logger.getLogger(ValkeyDBLockProviderFactory.class);

    private volatile ValkeyDBLockConfig config = ValkeyDBLockConfig.from(Config.scope("dblock", PROVIDER_ID));
    private final AtomicBoolean forcedUnlock = new AtomicBoolean(false);

    @Override
    public void init(Config.Scope scope) {
        Config.Scope effective = scope != null ? scope : Config.scope("dblock", PROVIDER_ID);
        this.config = ValkeyDBLockConfig.from(effective);
        logger.debugf("Valkey DB lock provider configured with namespace=%s waitTimeout=%s recheckInterval=%s", config.getNamespace(),
                config.getWaitTimeout(), config.getRecheckInterval());
    }

    @Override
    public void postInit(KeycloakSessionFactory factory) {
        // No-op.
    }

    @Override
    public DBLockProvider create(KeycloakSession session) {
        ValkeyConnectionProvider connections = session.getProvider(ValkeyConnectionProvider.class);
        if (connections == null) {
            throw new IllegalStateException("No Valkey connection provider available. Ensure keycloak-valkey connection factory is configured.");
        }
        maybeForceUnlock(connections);
        return new ValkeyDBLockProvider(connections.getConnection(), config);
    }

    private void maybeForceUnlock(ValkeyConnectionProvider connections) {
        if (config.isForceUnlockOnStartup() && forcedUnlock.compareAndSet(false, true)) {
            logger.warn("Force unlocking Valkey DB lock state at startup as requested by configuration");
            new ValkeyDBLockProvider(connections.getConnection(), config).destroyLockInfo();
        }
    }

    @Override
    public void setTimeouts(long lockRecheckTimeMillis, long lockWaitTimeoutMillis) {
        if (lockRecheckTimeMillis <= 0 || lockWaitTimeoutMillis <= 0) {
            throw new IllegalArgumentException("Timeouts must be positive");
        }
        this.config = config.withTimeouts(java.time.Duration.ofMillis(lockRecheckTimeMillis),
                java.time.Duration.ofMillis(lockWaitTimeoutMillis));
    }

    @Override
    public void close() {
        forcedUnlock.set(false);
    }

    @Override
    public String getId() {
        return PROVIDER_ID;
    }

    @Override
    public int order() {
        return 0;
    }

    @Override
    public List<ProviderConfigProperty> getConfigMetadata() {
        return ProviderConfigurationBuilder.create()
                .property()
                .name("namespace")
                .type("string")
                .helpText("Namespace prefix to use for Valkey DB lock keys")
                .add()
                .property()
                .name("lock-recheck-time")
                .type("string")
                .helpText("Interval between lock acquisition retries (ISO-8601 duration or milliseconds)")
                .add()
                .property()
                .name("lock-wait-timeout")
                .type("string")
                .helpText("Maximum time to wait for a DB lock before failing (ISO-8601 duration or milliseconds)")
                .add()
                .property()
                .name("lock-lease")
                .type("string")
                .helpText("TTL applied to the Valkey lock keys to protect against stale holders (ISO-8601 duration or milliseconds)")
                .add()
                .property()
                .name("force-unlock-on-startup")
                .type("boolean")
                .helpText("Forcefully clears residual Valkey DB lock keys during server startup")
                .add()
                .build();
    }
}
