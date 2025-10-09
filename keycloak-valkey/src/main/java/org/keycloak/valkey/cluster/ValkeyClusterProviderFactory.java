package org.keycloak.valkey.cluster;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.keycloak.Config;
import org.keycloak.cluster.ClusterProvider;
import org.keycloak.cluster.ClusterProviderFactory;
import org.keycloak.executors.ExecutorsProvider;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.valkey.ValkeyConnectionProvider;
import org.keycloak.valkey.config.ValkeyClusterConfig;

import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.RedisClient;

/**
 * Factory wiring the Valkey-backed {@link ClusterProvider} implementation.
 */
public class ValkeyClusterProviderFactory implements ClusterProviderFactory {

    public static final String PROVIDER_ID = "valkey";

    private final AtomicReference<Integer> clusterStartupTime = new AtomicReference<>();
    private final AtomicReference<ExecutorService> executorRef = new AtomicReference<>();
    private final AtomicReference<ValkeyClusterProvider> providerRef = new AtomicReference<>();

    private final String nodeId = UUID.randomUUID().toString();
    private volatile ValkeyClusterConfig config = ValkeyClusterConfig.from(null);

    @Override
    public ClusterProvider create(KeycloakSession session) {
        ValkeyClusterProvider existing = providerRef.get();
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            ValkeyClusterProvider current = providerRef.get();
            if (current != null) {
                return current;
            }
            ValkeyConnectionProvider connectionProvider = session.getProvider(ValkeyConnectionProvider.class);
            if (connectionProvider == null) {
                throw new IllegalStateException("ValkeyConnectionProvider must be available");
            }
            ExecutorService executor = resolveExecutor(session);
            RedisClient client = connectionProvider.getRedisClient();
            int startup = ensureClusterStartupTime(session, client);
            ValkeyClusterProvider created = new ValkeyClusterProvider(nodeId, startup, client, config, executor);
            providerRef.set(created);
            return created;
        }
    }

    @Override
    public void init(Config.Scope scope) {
        this.config = ValkeyClusterConfig.from(scope);
    }

    @Override
    public void postInit(KeycloakSessionFactory factory) {
        // No-op.
    }

    @Override
    public void close() {
        clusterStartupTime.set(null);
        executorRef.set(null);
        ValkeyClusterProvider provider = providerRef.getAndSet(null);
        if (provider != null) {
            provider.shutdown();
        }
    }

    @Override
    public String getId() {
        return PROVIDER_ID;
    }

    private ExecutorService resolveExecutor(KeycloakSession session) {
        ExecutorService executor = executorRef.get();
        if (executor != null && !executor.isShutdown()) {
            return executor;
        }

        ExecutorsProvider executorsProvider = session.getProvider(ExecutorsProvider.class);
        ExecutorService created = executorsProvider != null
                ? executorsProvider.getExecutor("cluster-valkey")
                : null;
        if (created == null) {
            throw new IllegalStateException("ExecutorsProvider must be available for ValkeyClusterProvider");
        }
        executorRef.compareAndSet(null, created);
        return executorRef.get();
    }

    private int ensureClusterStartupTime(KeycloakSession session, RedisClient client) {
        Integer cached = clusterStartupTime.get();
        if (cached != null) {
            return cached;
        }

        synchronized (clusterStartupTime) {
            Integer existing = clusterStartupTime.get();
            if (existing != null) {
                return existing;
            }
            int computed = computeClusterStartupTime(session, client);
            clusterStartupTime.set(computed);
            return computed;
        }
    }

    private int computeClusterStartupTime(KeycloakSession session, RedisClient client) {
        long startupMillis = session.getKeycloakSessionFactory().getServerStartupTimestamp();
        int startupSeconds = (int) (startupMillis / 1000);
        String key = config.startupKey();
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            String result = connection.sync().set(key, Integer.toString(startupSeconds), SetArgs.Builder.nx());
            if (result == null) {
                String stored = connection.sync().get(key);
                if (stored == null) {
                    throw new IllegalStateException("Unable to determine cluster startup time from Valkey");
                }
                return Integer.parseInt(stored);
            }
            return startupSeconds;
        }
    }
}
