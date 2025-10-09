package org.keycloak.valkey.connection;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.keycloak.Config;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.provider.ServerInfoAwareProviderFactory;
import org.keycloak.valkey.ValkeyConnectionProvider;
import org.keycloak.valkey.ValkeyConnectionProviderFactory;
import org.keycloak.valkey.ValkeyConnectionSpi;
import org.keycloak.valkey.config.ValkeyConfig;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.metrics.CommandLatencyCollectorOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;

/**
 * Factory that manages a shared {@link RedisClient} and exposes session-scoped connection providers.
 */
public class LettuceValkeyConnectionProviderFactory implements ValkeyConnectionProviderFactory, ServerInfoAwareProviderFactory {

    public static final String PROVIDER_ID = "lettuce";

    private volatile ValkeyConfig config;
    private volatile RedisClient redisClient;
    private volatile ClientResources clientResources;
    private final AtomicReference<ValkeyHealth> lastHealth = new AtomicReference<>(ValkeyHealth.notRun());

    public LettuceValkeyConnectionProviderFactory() {
        this.config = ValkeyConfig.from(Config.scope(ValkeyConnectionSpi.SPI_NAME, PROVIDER_ID));
    }

    @Override
    public ValkeyConnectionProvider create(KeycloakSession session) {
        return new LettuceValkeyConnectionProvider(ensureClient(), config);
    }

    private synchronized RedisClient ensureClient() {
        if (redisClient != null) {
            return redisClient;
        }

        ClientResources resources = DefaultClientResources.builder()
                .commandLatencyCollectorOptions(CommandLatencyCollectorOptions.disabled())
                .build();
        this.clientResources = resources;

        RedisClient client = RedisClient.create(resources, config.toRedisURI());
        client.setOptions(buildClientOptions(config));
        client.setDefaultTimeout(config.getCommandTimeout());
        this.redisClient = client;
        return client;
    }

    private ClientOptions buildClientOptions(ValkeyConfig config) {
        TimeoutOptions timeoutOptions = TimeoutOptions.enabled(config.getCommandTimeout());
        SocketOptions socketOptions = SocketOptions.builder()
                .connectTimeout(config.getConnectTimeout())
                .keepAlive(true)
                .tcpNoDelay(true)
                .build();

        return ClientOptions.builder()
                .autoReconnect(true)
                .cancelCommandsOnReconnectFailure(true)
                .pingBeforeActivateConnection(true)
                .suspendReconnectOnProtocolFailure(true)
                .requestQueueSize(config.getRequestQueueSize())
                .timeoutOptions(timeoutOptions)
                .socketOptions(socketOptions)
                .build();
    }

    @Override
    public void init(Config.Scope scope) {
        Config.Scope effectiveScope = scope != null ? scope : Config.scope(ValkeyConnectionSpi.SPI_NAME, PROVIDER_ID);
        this.config = ValkeyConfig.from(effectiveScope);
    }

    @Override
    public void postInit(KeycloakSessionFactory factory) {
        // No-op.
    }

    @Override
    public void close() {
        RedisClient client = this.redisClient;
        this.redisClient = null;
        if (client != null) {
            shutdownClient(client, config.getShutdownQuietPeriod(), config.getShutdownTimeout());
        }

        ClientResources resources = this.clientResources;
        this.clientResources = null;
        if (resources != null) {
            resources.shutdown(config.getShutdownQuietPeriod().toMillis(), config.getShutdownTimeout().toMillis(), TimeUnit.MILLISECONDS);
        }
        lastHealth.set(ValkeyHealth.notRun());
    }

    private void shutdownClient(RedisClient client, Duration quietPeriod, Duration timeout) {
        try {
            client.shutdown(quietPeriod, timeout);
        } catch (RuntimeException ex) {
            client.shutdown();
        }
    }

    @Override
    public String getId() {
        return PROVIDER_ID;
    }

    @Override
    public Map<String, String> getOperationalInfo() {
        ValkeyConfig currentConfig = this.config;
        Map<String, String> info = new LinkedHashMap<>();
        info.put("config.uri", currentConfig.getSanitizedUri());
        if (currentConfig.getClientName() != null) {
            info.put("config.clientName", currentConfig.getClientName());
        }
        info.put("config.ssl", Boolean.toString(currentConfig.isSsl()));
        info.put("config.startTls", Boolean.toString(currentConfig.isStartTls()));
        info.put("config.verifyPeer", Boolean.toString(currentConfig.isVerifyPeer()));
        info.put("config.connectTimeoutMs", Long.toString(currentConfig.getConnectTimeout().toMillis()));
        info.put("config.commandTimeoutMs", Long.toString(currentConfig.getCommandTimeout().toMillis()));
        info.put("config.shutdownQuietPeriodMs", Long.toString(currentConfig.getShutdownQuietPeriod().toMillis()));
        info.put("config.shutdownTimeoutMs", Long.toString(currentConfig.getShutdownTimeout().toMillis()));
        info.put("config.requestQueueSize", Integer.toString(currentConfig.getRequestQueueSize()));

        ValkeyHealth health = probeHealth();
        info.put("health.status", health.status.name());
        info.put("health.latencyMs", Long.toString(health.latencyMs));
        info.put("health.timestamp", health.timestamp.toString());
        if (health.errorMessage != null) {
            info.put("health.error", health.errorMessage);
        }
        return info;
    }

    private ValkeyHealth probeHealth() {
        long start = System.nanoTime();
        try (StatefulRedisConnection<String, String> connection = ensureClient().connect(StringCodec.UTF8)) {
            String response = connection.sync().ping();
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            ValkeyHealth health = ValkeyHealth.fromResponse(response, elapsed);
            lastHealth.set(health);
            return health;
        } catch (Exception ex) {
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            ValkeyHealth previous = lastHealth.get();
            ValkeyHealth health = ValkeyHealth.down(elapsed, ex.getMessage(), previous);
            lastHealth.set(health);
            return health;
        }
    }

    private static final class ValkeyHealth {
        private enum Status {
            UP, DOWN
        }

        private final Status status;
        private final long latencyMs;
        private final Instant timestamp;
        private final String errorMessage;

        private ValkeyHealth(Status status, long latencyMs, Instant timestamp, String errorMessage) {
            this.status = status;
            this.latencyMs = latencyMs;
            this.timestamp = timestamp;
            this.errorMessage = errorMessage;
        }

        private static ValkeyHealth fromResponse(String response, long latencyMs) {
            Status status = "PONG".equalsIgnoreCase(response) ? Status.UP : Status.DOWN;
            String error = status == Status.UP ? null : "Unexpected response: " + response;
            return new ValkeyHealth(status, latencyMs, Instant.now(), error);
        }

        private static ValkeyHealth down(long latencyMs, String error, ValkeyHealth previous) {
            Instant timestamp = Instant.now();
            String message = error != null && !error.isBlank() ? error : "Health probe failed";
            if (previous != null && previous.status == Status.UP && previous.timestamp != null && !Instant.EPOCH.equals(previous.timestamp)) {
                message = message + " (last success at " + previous.timestamp + ')';
            }
            return new ValkeyHealth(Status.DOWN, latencyMs, timestamp, message);
        }

        private static ValkeyHealth notRun() {
            return new ValkeyHealth(Status.DOWN, 0, Instant.EPOCH, "Health probe not yet executed");
        }
    }
}
