package org.keycloak.valkey.usersession;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import org.keycloak.util.JsonSerialization;

import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.sync.RedisCommands;

final class ValkeyUserSessionPersisterStore {

    private static final int MAX_UPDATE_ATTEMPTS = 8;

    private final RedisCommands<String, String> commands;
    private final String prefix;

    ValkeyUserSessionPersisterStore(RedisCommands<String, String> commands, String prefix) {
        this.commands = Objects.requireNonNull(commands, "commands");
        this.prefix = Objects.requireNonNull(prefix, "prefix");
    }

    void save(ValkeyUserSessionEntity entity) {
        update(entity.getId(), entity.isOffline(), current -> entity.copy());
    }

    Optional<ValkeyUserSessionEntity> load(String sessionId, boolean offline) {
        if (sessionId == null) {
            return Optional.empty();
        }
        String payload = commands.get(dataKey(offline, sessionId));
        if (payload == null) {
            return Optional.empty();
        }
        return Optional.of(decode(payload));
    }

    void delete(String sessionId, boolean offline) {
        update(sessionId, offline, current -> null);
    }

    void deleteRealm(String realmId) {
        if (realmId == null) {
            return;
        }
        removeRealmSessions(realmId, false);
        removeRealmSessions(realmId, true);
    }

    void removeRealmSessions(String realmId, boolean offline) {
        if (realmId == null) {
            return;
        }
        Set<String> sessionIds = commands.smembers(realmKey(offline, realmId));
        for (String sessionId : sessionIds) {
            delete(sessionId, offline);
        }
    }

    void removeClientSessions(String realmId, String clientId) {
        if (realmId == null || clientId == null) {
            return;
        }
        removeClientSessions(realmId, clientId, false);
        removeClientSessions(realmId, clientId, true);
    }

    void removeClientSessions(String realmId, String clientId, boolean offline) {
        Set<String> sessionIds = commands.smembers(clientKey(offline, realmId, clientId));
        for (String sessionId : sessionIds) {
            update(sessionId, offline, current -> {
                if (current == null) {
                    return null;
                }
                current.getClientSessions().remove(clientId);
                return current;
            });
        }
    }

    void removeUserSessions(String realmId, String userId) {
        if (realmId == null || userId == null) {
            return;
        }
        removeUserSessions(realmId, userId, false);
        removeUserSessions(realmId, userId, true);
    }

    void removeUserSessions(String realmId, String userId, boolean offline) {
        Set<String> sessionIds = commands.smembers(userKey(offline, realmId, userId));
        for (String sessionId : sessionIds) {
            delete(sessionId, offline);
        }
    }

    Stream<ValkeyUserSessionEntity> streamRealmSessions(String realmId) {
        if (realmId == null) {
            return Stream.empty();
        }
        Set<String> online = commands.smembers(realmKey(false, realmId));
        Set<String> offline = commands.smembers(realmKey(true, realmId));
        Set<String> all = new HashSet<>(online);
        all.addAll(offline);
        return all.stream()
                .map(sessionId -> load(sessionId, offline.contains(sessionId)))
                .flatMap(Optional::stream);
    }

    Stream<ValkeyUserSessionEntity> streamUserSessions(String realmId, String userId, boolean offline, Integer firstResult,
            Integer maxResults) {
        if (realmId == null || userId == null) {
            return Stream.empty();
        }
        List<String> ids = commands.smembers(userKey(offline, realmId, userId)).stream().sorted().toList();
        return paginate(ids, firstResult, maxResults).stream()
                .map(id -> load(id, offline))
                .flatMap(Optional::stream);
    }

    Stream<ValkeyUserSessionEntity> streamClientSessions(String realmId, String clientId, boolean offline, Integer firstResult,
            Integer maxResults) {
        if (realmId == null || clientId == null) {
            return Stream.empty();
        }
        List<String> ids = commands.smembers(clientKey(offline, realmId, clientId)).stream().sorted().toList();
        return paginate(ids, firstResult, maxResults).stream()
                .map(id -> load(id, offline))
                .flatMap(Optional::stream);
    }

    Stream<ValkeyUserSessionEntity> streamAll(boolean offline, Integer firstResult, Integer maxResults, String lastSessionId) {
        Range<String> range;
        if (lastSessionId == null || lastSessionId.isBlank()) {
            range = Range.unbounded();
        } else {
            range = Range.from(Range.Boundary.excluding(lastSessionId), Range.Boundary.unbounded());
        }
        int offset = firstResult != null && firstResult > 0 ? firstResult : 0;
        int count = maxResults != null && maxResults >= 0 ? maxResults : Integer.MAX_VALUE;
        Limit limit = Limit.create(offset, count);
        List<String> ids = commands.zrangebylex(orderKey(offline), range, limit);
        return ids.stream()
                .map(id -> load(id, offline))
                .flatMap(Optional::stream);
    }

    int countAll(boolean offline) {
        return Optional.ofNullable(commands.zcard(orderKey(offline))).map(Long::intValue).orElse(0);
    }

    int countClientSessions(String realmId, String clientId, boolean offline) {
        if (realmId == null || clientId == null) {
            return 0;
        }
        return Optional.ofNullable(commands.scard(clientKey(offline, realmId, clientId))).map(Long::intValue).orElse(0);
    }

    Map<String, Long> countSessionsByClient(String realmId, boolean offline) {
        if (realmId == null) {
            return Collections.emptyMap();
        }
        Set<String> clientIds = commands.smembers(clientCatalogKey(offline, realmId));
        Map<String, Long> counts = new HashMap<>();
        for (String clientId : clientIds) {
            long value = Optional.ofNullable(commands.scard(clientKey(offline, realmId, clientId))).orElse(0L);
            if (value > 0) {
                counts.put(clientId, value);
            } else {
                commands.srem(clientCatalogKey(offline, realmId), clientId);
            }
        }
        return counts;
    }

    void update(String sessionId, boolean offline, UnaryOperator<ValkeyUserSessionEntity> mutator) {
        if (sessionId == null) {
            return;
        }
        String dataKey = dataKey(offline, sessionId);
        for (int attempt = 0; attempt < MAX_UPDATE_ATTEMPTS; attempt++) {
            commands.watch(dataKey);
            ValkeyUserSessionEntity current = load(sessionId, offline).map(ValkeyUserSessionEntity::copy).orElse(null);
            ValkeyUserSessionEntity updated;
            try {
                updated = mutator.apply(current);
            } catch (RuntimeException ex) {
                commands.unwatch();
                throw ex;
            }
            commands.multi();
            if (updated == null) {
                commands.del(dataKey);
                removeIndexes(current, offline);
            } else {
                persistEntity(updated, offline);
                syncIndexes(current, updated, offline);
            }
            TransactionResult result = commands.exec();
            if (result != null) {
                return;
            }
        }
        throw new IllegalStateException("Failed to update persistent session after " + MAX_UPDATE_ATTEMPTS + " attempts");
    }

    private void persistEntity(ValkeyUserSessionEntity entity, boolean offline) {
        try {
            String json = JsonSerialization.writeValueAsString(entity);
            commands.set(dataKey(offline, entity.getId()), json);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to encode persistent session", ex);
        }
    }

    private void syncIndexes(ValkeyUserSessionEntity previous, ValkeyUserSessionEntity updated, boolean offline) {
        commands.sadd(realmKey(offline, updated.getRealmId()), updated.getId());
        commands.sadd(userKey(offline, updated.getRealmId(), updated.getUserId()), updated.getId());
        commands.zadd(orderKey(offline), 0, updated.getId());
        Set<String> retained = new HashSet<>();
        for (ValkeyAuthenticatedClientSessionEntity client : updated.getClientSessions().values()) {
            String clientKey = clientKey(offline, updated.getRealmId(), client.getClientId());
            commands.sadd(clientKey, updated.getId());
            commands.sadd(clientCatalogKey(offline, updated.getRealmId()), client.getClientId());
            retained.add(client.getClientId());
        }
        if (previous != null) {
            for (String clientId : previous.getClientSessions().keySet()) {
                if (!retained.contains(clientId)) {
                    commands.srem(clientKey(offline, previous.getRealmId(), clientId), previous.getId());
                }
            }
        }
    }

    private void removeIndexes(ValkeyUserSessionEntity entity, boolean offline) {
        if (entity == null) {
            return;
        }
        commands.srem(realmKey(offline, entity.getRealmId()), entity.getId());
        commands.srem(userKey(offline, entity.getRealmId(), entity.getUserId()), entity.getId());
        commands.zrem(orderKey(offline), entity.getId());
        for (String clientId : entity.getClientSessions().keySet()) {
            commands.srem(clientKey(offline, entity.getRealmId(), clientId), entity.getId());
        }
    }

    private List<String> paginate(List<String> ids, Integer firstResult, Integer maxResults) {
        int from = firstResult != null && firstResult > 0 ? firstResult : 0;
        int to = maxResults != null && maxResults >= 0 ? Math.min(ids.size(), from + maxResults) : ids.size();
        if (from >= ids.size()) {
            return List.of();
        }
        return ids.subList(from, to);
    }

    private String dataKey(boolean offline, String sessionId) {
        return prefix + ':' + (offline ? "offline" : "online") + ":session:" + sessionId;
    }

    private String realmKey(boolean offline, String realmId) {
        return prefix + ':' + (offline ? "offline" : "online") + ":realm:" + realmId;
    }

    private String userKey(boolean offline, String realmId, String userId) {
        return prefix + ':' + (offline ? "offline" : "online") + ":realm:" + realmId + ":user:" + userId;
    }

    private String clientKey(boolean offline, String realmId, String clientId) {
        return prefix + ':' + (offline ? "offline" : "online") + ":realm:" + realmId + ":client:" + clientId;
    }

    private String clientCatalogKey(boolean offline, String realmId) {
        return prefix + ':' + (offline ? "offline" : "online") + ":realm:" + realmId + ":clients";
    }

    private String orderKey(boolean offline) {
        return prefix + ':' + (offline ? "offline" : "online") + ":order";
    }

    private ValkeyUserSessionEntity decode(String payload) {
        try {
            return JsonSerialization.readValue(payload, ValkeyUserSessionEntity.class);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to decode persistent session", ex);
        }
    }
}
