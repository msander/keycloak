package org.keycloak.valkey.usersession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import org.jboss.logging.Logger;
import org.keycloak.common.util.Time;
import org.keycloak.models.RealmModel;
import org.keycloak.models.utils.SessionExpirationUtils;
import org.keycloak.util.JsonSerialization;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.SetArgs;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.sync.RedisCommands;

final class ValkeyUserSessionStore {

    private static final Logger logger = Logger.getLogger(ValkeyUserSessionStore.class);
    private static final int MAX_UPDATE_ATTEMPTS = 8;

    private final RedisCommands<String, String> commands;
    private final String onlinePrefix;
    private final String offlinePrefix;

    ValkeyUserSessionStore(RedisCommands<String, String> commands, String keyPrefix) {
        this.commands = Objects.requireNonNull(commands, "commands");
        Objects.requireNonNull(keyPrefix, "keyPrefix");
        this.onlinePrefix = keyPrefix + ":user-session";
        this.offlinePrefix = keyPrefix + ":offline-user-session";
    }

    Optional<ValkeyUserSessionEntity> load(String realmId, String id, boolean offline) {
        Objects.requireNonNull(realmId, "realmId");
        Objects.requireNonNull(id, "id");
        String payload = commands.get(key(offline, realmId, id));
        if (payload == null) {
            return Optional.empty();
        }
        return Optional.of(decode(payload));
    }

    ValkeyUserSessionEntity create(RealmModel realm, ValkeyUserSessionEntity entity) {
        Objects.requireNonNull(realm, "realm");
        Objects.requireNonNull(entity, "entity");
        String key = key(entity.isOffline(), realm.getId(), entity.getId());
        if (!persist(realm, key, entity)) {
            commands.del(key);
            return null;
        }
        return entity.copy();
    }

    ValkeyUserSessionEntity update(RealmModel realm, String id, boolean offline,
            UnaryOperator<ValkeyUserSessionEntity> mutator) {
        Objects.requireNonNull(realm, "realm");
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(mutator, "mutator");
        String key = key(offline, realm.getId(), id);
        for (int attempt = 0; attempt < MAX_UPDATE_ATTEMPTS; attempt++) {
            commands.watch(key);
            ValkeyUserSessionEntity current = load(realm.getId(), id, offline).map(ValkeyUserSessionEntity::copy).orElse(null);
            ValkeyUserSessionEntity updated;
            try {
                updated = mutator.apply(current);
            } catch (RuntimeException ex) {
                commands.unwatch();
                throw ex;
            }
            commands.multi();
            if (updated == null) {
                commands.del(key);
            } else {
                persistWithinTransaction(realm, key, updated);
            }
            TransactionResult result = commands.exec();
            if (result != null) {
                return updated == null ? null : updated.copy();
            }
        }
        throw new IllegalStateException("Failed to update user session after " + MAX_UPDATE_ATTEMPTS + " attempts");
    }

    void touch(RealmModel realm, String id, boolean offline, long refreshMillis) {
        Objects.requireNonNull(realm, "realm");
        Objects.requireNonNull(id, "id");
        String key = key(offline, realm.getId(), id);
        for (int attempt = 0; attempt < MAX_UPDATE_ATTEMPTS; attempt++) {
            commands.watch(key);
            ValkeyUserSessionEntity current = load(realm.getId(), id, offline).orElse(null);
            if (current == null) {
                commands.unwatch();
                return;
            }
            long now = Time.currentTimeMillis();
            long effectiveRefresh = refreshMillis > 0 ? Math.max(refreshMillis, now) : now;
            if (effectiveRefresh > current.getLastSessionRefreshMillis()) {
                current.setLastSessionRefreshMillis(effectiveRefresh);
            } else {
                current.setLastSessionRefreshMillis(Math.max(current.getLastSessionRefreshMillis(), now));
            }
            commands.multi();
            persistWithinTransaction(realm, key, current);
            TransactionResult result = commands.exec();
            if (result != null) {
                return;
            }
        }
        throw new IllegalStateException("Failed to refresh user session TTL after " + MAX_UPDATE_ATTEMPTS + " attempts");
    }

    void delete(String realmId, String id, boolean offline) {
        commands.del(key(offline, realmId, id));
    }

    void deleteAllForRealm(String realmId) {
        Objects.requireNonNull(realmId, "realmId");
        deleteByPattern(key(false, realmId, "*"));
        deleteByPattern(key(true, realmId, "*"));
    }

    Stream<ValkeyUserSessionEntity> stream(String realmId, boolean offline) {
        Objects.requireNonNull(realmId, "realmId");
        String pattern = key(offline, realmId, "*");
        ScanCursor cursor = ScanCursor.INITIAL;
        ScanArgs args = ScanArgs.Builder.matches(pattern).limit(200);
        List<ValkeyUserSessionEntity> entities = new ArrayList<>();
        do {
            KeyScanCursor<String> scan = commands.scan(cursor, args);
            for (String key : scan.getKeys()) {
                String payload = commands.get(key);
                if (payload != null) {
                    try {
                        entities.add(JsonSerialization.readValue(payload, ValkeyUserSessionEntity.class));
                    } catch (IOException ex) {
                        logger.warnf(ex, "Failed to decode user session payload for key %s", key);
                    }
                }
            }
            cursor = scan;
        } while (!cursor.isFinished());
        return entities.stream();
    }

    void removeMatching(String realmId, boolean offline, Predicate<ValkeyUserSessionEntity> predicate) {
        stream(realmId, offline)
                .filter(predicate)
                .map(entity -> key(entity.isOffline(), realmId, entity.getId()))
                .forEach(commands::del);
    }

    private boolean persist(RealmModel realm, String key, ValkeyUserSessionEntity entity) {
        try {
            String payload = JsonSerialization.writeValueAsString(entity);
            int ttlSeconds = computeTtlSeconds(realm, entity);
            if (ttlSeconds <= 0) {
                commands.del(key);
                return true;
            }
            SetArgs args = SetArgs.Builder.ex(ttlSeconds);
            String result = commands.set(key, payload, args);
            return "OK".equalsIgnoreCase(result);
        } catch (IOException ex) {
            logger.warnf(ex, "Failed to encode user session %s", entity.getId());
            return false;
        } catch (RedisCommandExecutionException ex) {
            logger.warnf(ex, "Failed to persist user session %s", entity.getId());
            return false;
        }
    }

    private void persistWithinTransaction(RealmModel realm, String key, ValkeyUserSessionEntity entity) {
        try {
            String payload = JsonSerialization.writeValueAsString(entity);
            int ttlSeconds = computeTtlSeconds(realm, entity);
            if (ttlSeconds <= 0) {
                commands.del(key);
                return;
            }
            commands.set(key, payload, SetArgs.Builder.ex(ttlSeconds));
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to encode user session", ex);
        }
    }

    private int computeTtlSeconds(RealmModel realm, ValkeyUserSessionEntity entity) {
        long nowMillis = Time.currentTimeMillis();
        long createdMillis = entity.getStarted() * 1000L;
        long lastRefreshMillis = entity.getLastSessionRefreshMillis();
        if (lastRefreshMillis <= 0) {
            lastRefreshMillis = entity.getLastSessionRefresh() * 1000L;
        }
        long idleMillis = Math.max(lastRefreshMillis, entity.getLastSessionRefresh() * 1000L);
        boolean offline = entity.isOffline();
        long lifespan = SessionExpirationUtils.calculateUserSessionMaxLifespanTimestamp(offline, entity.isRememberMe(),
                createdMillis, realm);
        long idle = SessionExpirationUtils.calculateUserSessionIdleTimestamp(offline, entity.isRememberMe(), idleMillis, realm);
        long expiry = -1;
        if (lifespan > 0) {
            expiry = lifespan;
        }
        if (idle > 0) {
            expiry = expiry < 0 ? idle : Math.min(expiry, idle);
        }
        if (expiry < 0) {
            return -1;
        }
        long seconds = (expiry - nowMillis + 999) / 1000;
        return (int) Math.max(1, Math.min(Integer.MAX_VALUE, seconds));
    }

    private String key(boolean offline, String realmId, String id) {
        String prefix = offline ? offlinePrefix : onlinePrefix;
        return prefix + ":" + realmId + ":" + id;
    }

    private ValkeyUserSessionEntity decode(String payload) {
        try {
            return JsonSerialization.readValue(payload, ValkeyUserSessionEntity.class);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to decode user session payload", ex);
        }
    }

    private void deleteByPattern(String pattern) {
        ScanCursor cursor = ScanCursor.INITIAL;
        ScanArgs args = ScanArgs.Builder.matches(pattern).limit(200);
        do {
            KeyScanCursor<String> scan = commands.scan(cursor, args);
            List<String> keys = new ArrayList<>(scan.getKeys());
            if (!keys.isEmpty()) {
                commands.del(keys.toArray(new String[0]));
            }
            cursor = scan;
        } while (!cursor.isFinished());
    }
}
