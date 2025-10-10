package org.keycloak.valkey.authsession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.UnaryOperator;

import org.jboss.logging.Logger;
import org.keycloak.common.util.Time;
import org.keycloak.models.RealmModel;
import org.keycloak.models.utils.SessionExpiration;
import org.keycloak.util.JsonSerialization;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.sync.RedisCommands;

/**
 * Persists root authentication sessions and their tab sessions in Valkey.
 */
final class ValkeyAuthenticationSessionStore {

    private static final Logger logger = Logger.getLogger(ValkeyAuthenticationSessionStore.class);
    private static final int MAX_UPDATE_ATTEMPTS = 8;

    private final RedisCommands<String, String> commands;
    private final String keyPrefix;

    ValkeyAuthenticationSessionStore(RedisCommands<String, String> commands, String keyPrefix) {
        this.commands = Objects.requireNonNull(commands, "commands");
        this.keyPrefix = Objects.requireNonNull(keyPrefix, "keyPrefix");
    }

    Optional<ValkeyRootAuthenticationSessionEntity> load(String realmId, String id) {
        Objects.requireNonNull(realmId, "realmId");
        Objects.requireNonNull(id, "id");
        String payload = commands.get(key(realmId, id));
        if (payload == null) {
            return Optional.empty();
        }
        return Optional.of(decode(payload));
    }

    Optional<ValkeyRootAuthenticationSessionEntity> loadById(String id) {
        Objects.requireNonNull(id, "id");
        String pattern = keyPrefix + ":*:" + id;
        ScanCursor cursor = ScanCursor.INITIAL;
        ScanArgs args = ScanArgs.Builder.matches(pattern).limit(200);
        do {
            KeyScanCursor<String> scan = commands.scan(cursor, args);
            for (String key : scan.getKeys()) {
                String payload = commands.get(key);
                if (payload != null) {
                    return Optional.of(decode(payload));
                }
            }
            cursor = scan;
        } while (!cursor.isFinished());
        return Optional.empty();
    }

    ValkeyRootAuthenticationSessionEntity create(RealmModel realm, ValkeyRootAuthenticationSessionEntity entity) {
        Objects.requireNonNull(realm, "realm");
        Objects.requireNonNull(entity, "entity");
        String key = key(realm.getId(), entity.getId());
        if (!persist(realm, key, entity)) {
            commands.del(key);
            return null;
        }
        return entity.copy();
    }

    ValkeyRootAuthenticationSessionEntity update(RealmModel realm, String id,
            UnaryOperator<ValkeyRootAuthenticationSessionEntity> updater) {
        Objects.requireNonNull(realm, "realm");
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(updater, "updater");
        String key = key(realm.getId(), id);

        for (int attempt = 0; attempt < MAX_UPDATE_ATTEMPTS; attempt++) {
            commands.watch(key);
            ValkeyRootAuthenticationSessionEntity current = load(realm.getId(), id).map(ValkeyRootAuthenticationSessionEntity::copy)
                    .orElse(null);
            ValkeyRootAuthenticationSessionEntity updated;
            try {
                updated = updater.apply(current);
            } catch (RuntimeException ex) {
                commands.unwatch();
                throw ex;
            }

            commands.multi();
            if (updated == null) {
                commands.del(key);
            } else {
                updated.setRealmId(realm.getId());
                persistWithinTransaction(realm, key, updated);
            }

            TransactionResult result = commands.exec();
            if (result != null) {
                return updated == null ? null : updated.copy();
            }
        }

        throw new IllegalStateException("Failed to update authentication session after " + MAX_UPDATE_ATTEMPTS + " attempts");
    }

    void delete(String realmId, String id) {
        commands.del(key(realmId, id));
    }

    void deleteAllForRealm(String realmId) {
        Objects.requireNonNull(realmId, "realmId");
        String pattern = key(realmId, "*");
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

    void updateAuthNotes(RealmModel realm, String id, String tabId, Map<String, String> fragment) {
        update(realm, id, current -> {
            if (current == null) {
                return null;
            }
            ValkeyAuthenticationSessionEntity session = current.getAuthenticationSessions().get(tabId);
            if (session == null) {
                return current;
            }
            ValkeyAuthenticationSessionEntity copy = session.copy();
            fragment.forEach((key, value) -> {
                if (value == null) {
                    copy.getAuthNotes().remove(key);
                } else {
                    copy.getAuthNotes().put(key, value);
                }
            });
            current.getAuthenticationSessions().put(tabId, copy);
            return current;
        });
    }

    private void persistWithinTransaction(RealmModel realm, String key, ValkeyRootAuthenticationSessionEntity entity) {
        int ttlSeconds = computeTtlSeconds(realm, entity);
        if (ttlSeconds <= 0) {
            commands.del(key);
            return;
        }
        try {
            commands.set(key, encode(entity), SetArgs.Builder.ex(ttlSeconds));
        } catch (RedisCommandExecutionException ex) {
            logger.debugf(ex, "Failed to persist authentication session %s", key);
            throw ex;
        }
    }

    private boolean persist(RealmModel realm, String key, ValkeyRootAuthenticationSessionEntity entity) {
        int ttlSeconds = computeTtlSeconds(realm, entity);
        if (ttlSeconds <= 0) {
            return false;
        }
        try {
            commands.set(key, encode(entity), SetArgs.Builder.ex(ttlSeconds));
            return true;
        } catch (RedisCommandExecutionException ex) {
            logger.debugf(ex, "Failed to persist authentication session %s", key);
            return false;
        }
    }

    private int computeTtlSeconds(RealmModel realm, ValkeyRootAuthenticationSessionEntity entity) {
        int lifespan = SessionExpiration.getAuthSessionLifespan(realm);
        if (lifespan <= 0) {
            return 1;
        }
        int now = Time.currentTime();
        int remaining = lifespan - Math.max(0, now - entity.getTimestamp());
        return remaining <= 0 ? 0 : remaining;
    }

    private String key(String realmId, String id) {
        return keyPrefix + ':' + realmId + ':' + id;
    }

    private static ValkeyRootAuthenticationSessionEntity decode(String payload) {
        try {
            return JsonSerialization.mapper.readValue(payload, ValkeyRootAuthenticationSessionEntity.class);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to decode Valkey authentication session", ex);
        }
    }

    private static String encode(ValkeyRootAuthenticationSessionEntity entity) {
        try {
            return JsonSerialization.writeValueAsString(entity);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to encode Valkey authentication session", ex);
        }
    }
}
