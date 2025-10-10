package org.keycloak.valkey.loginfailure;

import static org.keycloak.valkey.loginfailure.ValkeyLoginFailureRecord.FIELD_FAILED_LOGIN_NOT_BEFORE;
import static org.keycloak.valkey.loginfailure.ValkeyLoginFailureRecord.FIELD_LAST_FAILURE;
import static org.keycloak.valkey.loginfailure.ValkeyLoginFailureRecord.FIELD_LAST_IP_FAILURE;
import static org.keycloak.valkey.loginfailure.ValkeyLoginFailureRecord.FIELD_NUM_FAILURES;
import static org.keycloak.valkey.loginfailure.ValkeyLoginFailureRecord.FIELD_NUM_TEMPORARY_LOCKOUTS;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.models.UserLoginFailureModel;
import org.keycloak.models.UserLoginFailureProvider;
import org.keycloak.valkey.ValkeyConnectionProvider;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

/**
 * Redis-backed {@link UserLoginFailureProvider} that persists failure counters per realm and user.
 */
final class ValkeyUserLoginFailureProvider implements UserLoginFailureProvider {

    private static final String SET_IF_GREATER_SCRIPT =
            "local field = ARGV[1]; "
                    + "local candidate = tonumber(ARGV[2]); "
                    + "local current = redis.call('HGET', KEYS[1], field); "
                    + "if not current then redis.call('HSET', KEYS[1], field, candidate); return candidate; end; "
                    + "current = tonumber(current); "
                    + "if candidate > current then redis.call('HSET', KEYS[1], field, candidate); return candidate; end; "
                    + "return current;";

    private static final String CLEAR_FAILURES_SCRIPT =
            "redis.call('HSET', KEYS[1], ARGV[1], '0'); "
                    + "redis.call('HSET', KEYS[1], ARGV[2], '0'); "
                    + "redis.call('HSET', KEYS[1], ARGV[3], '0'); "
                    + "redis.call('HSET', KEYS[1], ARGV[4], '0'); "
                    + "redis.call('HDEL', KEYS[1], ARGV[5]); "
                    + "return 1;";

    private final RedisCommands<String, String> commands;
    private final String namespace;
    private final long minimumLifespanSeconds;

    ValkeyUserLoginFailureProvider(KeycloakSession session, ValkeyConnectionProvider connectionProvider, String namespace,
            long minimumLifespanSeconds) {
        Objects.requireNonNull(session, "session");
        ValkeyConnectionProvider provider = Objects.requireNonNull(connectionProvider, "connectionProvider");
        StatefulRedisConnection<String, String> connection = provider.getConnection();
        this.commands = connection.sync();
        this.namespace = Objects.requireNonNull(namespace, "namespace");
        if (minimumLifespanSeconds <= 0) {
            throw new IllegalArgumentException("minimumLifespanSeconds must be positive");
        }
        this.minimumLifespanSeconds = minimumLifespanSeconds;
    }

    @Override
    public UserLoginFailureModel getUserLoginFailure(RealmModel realm, String userId) {
        Objects.requireNonNull(realm, "realm");
        Objects.requireNonNull(userId, "userId");
        String key = key(realm.getId(), userId);
        ValkeyLoginFailureRecord record = loadRecord(key);
        return record != null ? new ValkeyUserLoginFailureAdapter(this, realm, key, record) : null;
    }

    @Override
    public UserLoginFailureModel addUserLoginFailure(RealmModel realm, String userId) {
        Objects.requireNonNull(realm, "realm");
        Objects.requireNonNull(userId, "userId");
        String key = key(realm.getId(), userId);
        if (commands.exists(key) == 0) {
            ValkeyLoginFailureRecord record = ValkeyLoginFailureRecord.empty(realm.getId(), userId);
            writeRecord(key, record);
        }
        refreshTtl(key, realm);
        ValkeyLoginFailureRecord record = loadRecord(key);
        if (record == null) {
            record = ValkeyLoginFailureRecord.empty(realm.getId(), userId);
            writeRecord(key, record);
        }
        return new ValkeyUserLoginFailureAdapter(this, realm, key, record);
    }

    @Override
    public void removeUserLoginFailure(RealmModel realm, String userId) {
        Objects.requireNonNull(realm, "realm");
        Objects.requireNonNull(userId, "userId");
        commands.del(key(realm.getId(), userId));
    }

    @Override
    public void removeAllUserLoginFailures(RealmModel realm) {
        Objects.requireNonNull(realm, "realm");
        String pattern = key(realm.getId(), "*");
        ScanCursor cursor = ScanCursor.INITIAL;
        ScanArgs args = ScanArgs.Builder.matches(pattern).limit(200);
        do {
            KeyScanCursor<String> scan = commands.scan(cursor, args);
            List<String> keys = scan.getKeys();
            if (!keys.isEmpty()) {
                commands.del(keys.toArray(new String[0]));
            }
            cursor = scan;
        } while (!cursor.isFinished());
    }

    @Override
    public void close() {
        // Connection lifecycle is managed by ValkeyConnectionProvider.
    }

    int incrementFailures(String key, RealmModel realm) {
        long updated = commands.hincrby(key, FIELD_NUM_FAILURES, 1L);
        refreshTtl(key, realm);
        return toInt(updated);
    }

    int incrementTemporaryLockouts(String key, RealmModel realm) {
        long updated = commands.hincrby(key, FIELD_NUM_TEMPORARY_LOCKOUTS, 1L);
        refreshTtl(key, realm);
        return toInt(updated);
    }

    int updateFailedLoginNotBefore(String key, RealmModel realm, int candidate) {
        long updated = commands.eval(SET_IF_GREATER_SCRIPT, ScriptOutputType.INTEGER, new String[] {key},
                FIELD_FAILED_LOGIN_NOT_BEFORE, Integer.toString(candidate));
        refreshTtl(key, realm);
        return toInt(updated);
    }

    long updateLastFailure(String key, RealmModel realm, long candidate) {
        long updated = commands.eval(SET_IF_GREATER_SCRIPT, ScriptOutputType.INTEGER, new String[] {key}, FIELD_LAST_FAILURE,
                Long.toString(candidate));
        refreshTtl(key, realm);
        return updated;
    }

    void updateLastIpFailure(String key, RealmModel realm, String ipAddress) {
        if (ipAddress == null || ipAddress.isBlank()) {
            commands.hdel(key, FIELD_LAST_IP_FAILURE);
        } else {
            commands.hset(key, FIELD_LAST_IP_FAILURE, ipAddress);
        }
        refreshTtl(key, realm);
    }

    ValkeyLoginFailureRecord clearFailures(String key, RealmModel realm) {
        commands.eval(CLEAR_FAILURES_SCRIPT, ScriptOutputType.INTEGER, new String[] {key},
                FIELD_FAILED_LOGIN_NOT_BEFORE,
                FIELD_NUM_FAILURES,
                FIELD_NUM_TEMPORARY_LOCKOUTS,
                FIELD_LAST_FAILURE,
                FIELD_LAST_IP_FAILURE);
        refreshTtl(key, realm);
        ValkeyLoginFailureRecord record = loadRecord(key);
        return record != null ? record.cleared() : null;
    }

    private ValkeyLoginFailureRecord loadRecord(String key) {
        Map<String, String> payload = commands.hgetall(key);
        return ValkeyLoginFailureRecord.from(payload);
    }

    private void writeRecord(String key, ValkeyLoginFailureRecord record) {
        Map<String, String> payload = record.toMap();
        if (payload.isEmpty()) {
            return;
        }
        for (Entry<String, String> entry : payload.entrySet()) {
            commands.hset(key, entry.getKey(), entry.getValue());
        }
    }

    private void refreshTtl(String key, RealmModel realm) {
        long lifespan = Math.max(minimumLifespanSeconds, resolveRealmLifespanSeconds(realm));
        if (lifespan > 0) {
            commands.expire(key, lifespan);
        }
    }

    private long resolveRealmLifespanSeconds(RealmModel realm) {
        long maxDelta = Math.max(0, realm.getMaxDeltaTimeSeconds());
        long maxFailureWait = Math.max(0, realm.getMaxFailureWaitSeconds());
        long minimumQuickWait = Math.max(0, realm.getMinimumQuickLoginWaitSeconds());
        long quickLoginCheck = millisToSecondsCeiling(realm.getQuickLoginCheckMilliSeconds());
        return Math.max(Math.max(maxDelta, maxFailureWait), Math.max(minimumQuickWait, quickLoginCheck));
    }

    private String key(String realmId, String userId) {
        if (userId == null) {
            throw new IllegalArgumentException("userId must not be null");
        }
        String suffix = userId.equals("*") ? "*" : userId;
        return namespace + ':' + Objects.requireNonNull(realmId, "realmId") + ':' + suffix;
    }

    private static long millisToSecondsCeiling(long millis) {
        if (millis <= 0) {
            return 0;
        }
        return Duration.ofMillis(millis).plusMillis(999).toSeconds();
    }

    private static int toInt(long value) {
        if (value > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        if (value < Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        }
        return (int) value;
    }
}
