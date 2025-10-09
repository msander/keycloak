package org.keycloak.valkey.connection;

import java.util.concurrent.atomic.AtomicReference;

import org.keycloak.valkey.ValkeyConnectionProvider;
import org.keycloak.valkey.config.ValkeyConfig;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

/**
 * Session-scoped provider that lazily obtains Lettuce connections backed by a shared {@link RedisClient}.
 */
public class LettuceValkeyConnectionProvider implements ValkeyConnectionProvider {

    private final RedisClient redisClient;
    private final ValkeyConfig config;
    private final AtomicReference<StatefulRedisConnection<String, String>> stringConnection = new AtomicReference<>();
    private final AtomicReference<StatefulRedisConnection<String, byte[]>> binaryConnection = new AtomicReference<>();

    public LettuceValkeyConnectionProvider(RedisClient redisClient, ValkeyConfig config) {
        this.redisClient = redisClient;
        this.config = config;
    }

    @Override
    public RedisClient getRedisClient() {
        return redisClient;
    }

    @Override
    public StatefulRedisConnection<String, String> getConnection() {
        StatefulRedisConnection<String, String> existing = stringConnection.get();
        if (existing != null && existing.isOpen()) {
            return existing;
        }

        StatefulRedisConnection<String, String> created = redisClient.connect(StringCodec.UTF8);
        created.setAutoFlushCommands(true);
        if (!stringConnection.compareAndSet(existing, created)) {
            created.close();
        }
        return stringConnection.get();
    }

    @Override
    public StatefulRedisConnection<String, byte[]> getBinaryConnection() {
        StatefulRedisConnection<String, byte[]> existing = binaryConnection.get();
        if (existing != null && existing.isOpen()) {
            return existing;
        }

        RedisCodec<String, byte[]> codec = RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE);
        StatefulRedisConnection<String, byte[]> created = redisClient.connect(codec);
        created.setAutoFlushCommands(true);
        if (!binaryConnection.compareAndSet(existing, created)) {
            created.close();
        }
        return binaryConnection.get();
    }

    @Override
    public void close() {
        closeQuietly(stringConnection.getAndSet(null));
        closeQuietly(binaryConnection.getAndSet(null));
    }

    private void closeQuietly(StatefulRedisConnection<?, ?> connection) {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (RuntimeException ignored) {
            // Best-effort close to avoid masking shutdown sequences.
        }
    }

    @Override
    public String toString() {
        return "LettuceValkeyConnectionProvider{" + config + '}';
    }
}
