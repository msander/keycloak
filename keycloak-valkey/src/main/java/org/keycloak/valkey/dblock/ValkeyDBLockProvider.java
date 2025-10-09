package org.keycloak.valkey.dblock;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import org.jboss.logging.Logger;
import org.keycloak.models.dblock.DBLockProvider;
import org.keycloak.valkey.config.ValkeyDBLockConfig;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

/**
 * Redis/Valkey-backed implementation of {@link DBLockProvider}.
 */
public class ValkeyDBLockProvider implements DBLockProvider {

    private static final Logger logger = Logger.getLogger(ValkeyDBLockProvider.class);
    private static final String RELEASE_LOCK_SCRIPT =
            "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end";

    private final RedisCommands<String, String> commands;
    private final ValkeyDBLockConfig config;

    private Namespace currentLock;
    private String currentToken;
    private Instant lockAcquiredAt;

    ValkeyDBLockProvider(StatefulRedisConnection<String, String> connection, ValkeyDBLockConfig config) {
        this.commands = Objects.requireNonNull(connection, "connection").sync();
        this.config = Objects.requireNonNull(config, "config");
    }

    @Override
    public synchronized void waitForLock(Namespace lock) {
        Objects.requireNonNull(lock, "lock");
        if (currentLock == lock) {
            logger.warnf("Lock namespace %s already held by this provider", lock);
            return;
        }
        if (currentLock != null) {
            throw new RuntimeException("Trying to get a lock when one was already taken by the provider");
        }

        String key = config.keyFor(lock);
        String token = newToken();
        Duration waitTimeout = config.getWaitTimeout();
        Duration recheck = config.getRecheckInterval();
        long deadline = System.nanoTime() + waitTimeout.toNanos();
        long sleepMillis = Math.max(1L, recheck.toMillis());

        logger.debugf("Acquiring DB lock %s with key %s", lock, key);
        while (System.nanoTime() < deadline) {
            if (tryAcquireLock(key, token)) {
                currentLock = lock;
                currentToken = token;
                lockAcquiredAt = Instant.now();
                return;
            }
            sleepQuietly(sleepMillis);
        }

        throw new RuntimeException("Failed to acquire DB lock " + lock + " within " + waitTimeout);
    }

    private boolean tryAcquireLock(String key, String token) {
        try {
            String response = commands.set(key, token, SetArgs.Builder.nx().px(config.getLockLease().toMillis()));
            return "OK".equalsIgnoreCase(response);
        } catch (RuntimeException ex) {
            logger.debugf(ex, "Failed to acquire lock %s", key);
            sleepQuietly(config.getRecheckInterval().toMillis());
            return false;
        }
    }

    private static String newToken() {
        return Long.toHexString(ThreadLocalRandom.current().nextLong()) + '-' + System.nanoTime();
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for DB lock", ex);
        }
    }

    @Override
    public synchronized void releaseLock() {
        if (currentLock == null) {
            return;
        }
        String key = config.keyFor(currentLock);
        String token = currentToken;
        currentLock = null;
        currentToken = null;
        lockAcquiredAt = null;

        try {
            commands.eval(RELEASE_LOCK_SCRIPT, ScriptOutputType.INTEGER, new String[] {key}, token);
        } catch (RuntimeException ex) {
            logger.debugf(ex, "Failed to release lock %s", key);
        }
    }

    @Override
    public synchronized Namespace getCurrentLock() {
        return currentLock;
    }

    @Override
    public boolean supportsForcedUnlock() {
        return true;
    }

    @Override
    public void destroyLockInfo() {
        try {
            commands.del(config.allKeys());
        } catch (RuntimeException ex) {
            logger.warnf(ex, "Failed to destroy Valkey DB lock state");
        }
    }

    @Override
    public void close() {
        // No-op. Connections are managed by the underlying Valkey connection provider.
    }

    @Override
    public synchronized String toString() {
        return "ValkeyDBLockProvider{" +
                "currentLock=" + currentLock +
                ", acquiredAt=" + lockAcquiredAt +
                '}';
    }
}
