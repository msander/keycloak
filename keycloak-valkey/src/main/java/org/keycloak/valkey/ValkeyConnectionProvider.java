package org.keycloak.valkey;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;

import org.keycloak.provider.Provider;

/**
 * Provides access to Valkey/Redis connectivity primitives that can be reused by SPI implementations.
 * Implementations are expected to be lightweight session-scoped facades backed by a shared {@link RedisClient}.
 */
public interface ValkeyConnectionProvider extends Provider {

    /**
     * @return the shared {@link RedisClient} managed by the factory. The client remains owned by the factory
     *         and must not be shut down by callers.
     */
    RedisClient getRedisClient();

    /**
     * Lazily obtains a UTF-8 string encoded connection. Connections are cached for the lifespan of the provider and
     * closed when {@link #close()} is invoked.
     *
     * @return stateful connection configured for UTF-8 string commands.
     */
    StatefulRedisConnection<String, String> getConnection();

    /**
     * Lazily obtains a binary-safe connection using {@code byte[]} payloads. Connections are cached for the lifespan
     * of the provider and closed when {@link #close()} is invoked.
     *
     * @return binary connection for advanced use cases (session serialization, map storage, ...).
     */
    StatefulRedisConnection<String, byte[]> getBinaryConnection();
}
