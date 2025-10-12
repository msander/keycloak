package org.keycloak.valkey.usersession;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.keycloak.common.util.Time;
import org.keycloak.models.AuthenticatedClientSessionModel;
import org.keycloak.models.ClientModel;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.models.UserModel;
import org.keycloak.models.UserSessionModel;
import org.keycloak.models.UserSessionProvider;
import org.keycloak.models.utils.KeycloakModelUtils;
import org.keycloak.valkey.ValkeyConnectionProvider;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

final class ValkeyUserSessionProvider implements UserSessionProvider {

    private static final String KEY_PREFIX = "user-session";

    private final KeycloakSession session;
    private final ValkeyUserSessionStore store;
    private final int startupTime;

    ValkeyUserSessionProvider(KeycloakSession session, ValkeyConnectionProvider connectionProvider) {
        this.session = Objects.requireNonNull(session, "session");
        ValkeyConnectionProvider provider = Objects.requireNonNull(connectionProvider, "connectionProvider");
        StatefulRedisConnection<String, String> connection = provider.getConnection();
        RedisCommands<String, String> commands = connection.sync();
        this.store = new ValkeyUserSessionStore(commands, KEY_PREFIX);
        this.startupTime = Time.currentTime();
    }

    @Override
    public KeycloakSession getKeycloakSession() {
        return session;
    }

    @Override
    public AuthenticatedClientSessionModel createClientSession(RealmModel realm, ClientModel client, UserSessionModel userSession) {
        Objects.requireNonNull(realm, "realm");
        Objects.requireNonNull(client, "client");
        Objects.requireNonNull(userSession, "userSession");
        boolean offline = userSession.isOffline();
        if (userSession.getPersistenceState() == UserSessionModel.SessionPersistenceState.TRANSIENT
                && !(userSession instanceof ValkeyUserSessionAdapter)) {
            throw new IllegalStateException("Transient user sessions must be backed by the Valkey adapter");
        }
        ValkeyAuthenticatedClientSessionEntity newEntity = ValkeyAuthenticatedClientSessionEntity.create(realm, client, userSession);
        newEntity.setRealmId(realm.getId());
        newEntity.setClientId(client.getId());
        newEntity.setUserSessionId(userSession.getId());
        newEntity.setAuthMethod(client.getProtocol());
        ValkeyUserSessionAdapter owner = userSession instanceof ValkeyUserSessionAdapter valkeyAdapter
                ? valkeyAdapter : toAdapter(userSession.getRealm(), userSession.getId(), offline);
        if (userSession.getPersistenceState() == UserSessionModel.SessionPersistenceState.TRANSIENT && owner != null) {
            ValkeyAuthenticatedClientSessionEntity transientEntity = newEntity.copy();
            owner.getEntity().getClientSessions().put(client.getId(), transientEntity);
            return new ValkeyAuthenticatedClientSessionAdapter(this, realm, client, owner, offline, transientEntity.copy());
        }
        ValkeyAuthenticatedClientSessionEntity persisted = updateClientSession(userSession, client.getId(), offline,
                existing -> newEntity.copy());
        if (persisted == null) {
            throw new IllegalStateException("Failed to persist Valkey client session");
        }
        if (owner != null) {
            store.load(realm.getId(), userSession.getId(), offline).ifPresent(owner::refreshFromStore);
        }
        return new ValkeyAuthenticatedClientSessionAdapter(this, realm, client,
                owner != null ? owner : toAdapter(userSession.getRealm(), userSession.getId(), offline), offline, persisted.copy());
    }

    @Override
    public AuthenticatedClientSessionModel getClientSession(UserSessionModel userSession, ClientModel client, boolean offline) {
        if (userSession == null || client == null) {
            return null;
        }
        ValkeyUserSessionAdapter adapter = toAdapter(userSession.getRealm(), userSession.getId(), offline);
        if (adapter == null) {
            return null;
        }
        ValkeyAuthenticatedClientSessionEntity entity = adapter.getEntity().getClientSessions().get(client.getId());
        if (entity == null) {
            return null;
        }
        return new ValkeyAuthenticatedClientSessionAdapter(this, userSession.getRealm(), client, adapter, offline, entity.copy());
    }

    @Override
    public UserSessionModel createUserSession(String id, RealmModel realm, UserModel user, String loginUsername, String ipAddress,
            String authMethod, boolean rememberMe, String brokerSessionId, String brokerUserId, UserSessionModel.SessionPersistenceState persistenceState) {
        Objects.requireNonNull(realm, "realm");
        Objects.requireNonNull(user, "user");
        UserSessionModel.SessionPersistenceState state = persistenceState == null ? UserSessionModel.SessionPersistenceState.PERSISTENT : persistenceState;
        String sessionId = (id == null || id.isBlank()) ? KeycloakModelUtils.generateId() : id;
        ValkeyUserSessionEntity entity = ValkeyUserSessionEntity.create(sessionId, realm, user, loginUsername, ipAddress, authMethod,
                rememberMe, brokerSessionId, brokerUserId, false);
        if (state == UserSessionModel.SessionPersistenceState.TRANSIENT) {
            return new ValkeyUserSessionAdapter(session, this, realm, entity, state);
        }
        ValkeyUserSessionEntity persisted = store.create(realm, entity);
        if (persisted == null) {
            throw new IllegalStateException("Failed to persist Valkey user session");
        }
        return new ValkeyUserSessionAdapter(session, this, realm, persisted, state);
    }

    @Override
    public UserSessionModel getUserSession(RealmModel realm, String id) {
        if (realm == null || id == null) {
            return null;
        }
        return store.load(realm.getId(), id, false)
                .map(entity -> new ValkeyUserSessionAdapter(session, this, realm, entity, UserSessionModel.SessionPersistenceState.PERSISTENT))
                .orElse(null);
    }

    @Override
    public Stream<UserSessionModel> getUserSessionsStream(RealmModel realm, UserModel user) {
        if (realm == null || user == null) {
            return Stream.empty();
        }
        String userId = user.getId();
        return store.stream(realm.getId(), false)
                .filter(entity -> userId.equals(entity.getUserId()))
                .map(entity -> new ValkeyUserSessionAdapter(session, this, realm, entity, UserSessionModel.SessionPersistenceState.PERSISTENT));
    }

    @Override
    public Stream<UserSessionModel> getUserSessionsStream(RealmModel realm, ClientModel client) {
        return getUserSessionsStream(realm, client, null, null);
    }

    @Override
    public Stream<UserSessionModel> getUserSessionsStream(RealmModel realm, ClientModel client, Integer firstResult, Integer maxResults) {
        if (realm == null || client == null) {
            return Stream.empty();
        }
        Stream<UserSessionModel> stream = store.stream(realm.getId(), false)
                .filter(entity -> entity.getClientSessions().containsKey(client.getId()))
                .map(entity -> new ValkeyUserSessionAdapter(session, this, realm, entity, UserSessionModel.SessionPersistenceState.PERSISTENT));
        if (firstResult != null && firstResult > 0) {
            stream = stream.skip(firstResult);
        }
        if (maxResults != null && maxResults >= 0) {
            stream = stream.limit(maxResults);
        }
        return stream;
    }

    @Override
    public Stream<UserSessionModel> getUserSessionByBrokerUserIdStream(RealmModel realm, String brokerUserId) {
        if (realm == null || brokerUserId == null) {
            return Stream.empty();
        }
        return store.stream(realm.getId(), false)
                .filter(entity -> brokerUserId.equals(entity.getBrokerUserId()))
                .map(entity -> new ValkeyUserSessionAdapter(session, this, realm, entity, UserSessionModel.SessionPersistenceState.PERSISTENT));
    }

    @Override
    public UserSessionModel getUserSessionByBrokerSessionId(RealmModel realm, String brokerSessionId) {
        if (realm == null || brokerSessionId == null) {
            return null;
        }
        return store.stream(realm.getId(), false)
                .filter(entity -> brokerSessionId.equals(entity.getBrokerSessionId()))
                .findFirst()
                .map(entity -> new ValkeyUserSessionAdapter(session, this, realm, entity, UserSessionModel.SessionPersistenceState.PERSISTENT))
                .orElse(null);
    }

    @Override
    public UserSessionModel getUserSessionWithPredicate(RealmModel realm, String id, boolean offline, Predicate<UserSessionModel> predicate) {
        if (realm == null || id == null || predicate == null) {
            return null;
        }
        return store.load(realm.getId(), id, offline)
                .map(entity -> new ValkeyUserSessionAdapter(session, this, realm, entity, UserSessionModel.SessionPersistenceState.PERSISTENT))
                .filter(predicate)
                .orElse(null);
    }

    @Override
    public long getActiveUserSessions(RealmModel realm, ClientModel client) {
        if (realm == null || client == null) {
            return 0;
        }
        return store.stream(realm.getId(), false)
                .filter(entity -> entity.getClientSessions().containsKey(client.getId()))
                .count();
    }

    @Override
    public Map<String, Long> getActiveClientSessionStats(RealmModel realm, boolean offline) {
        if (realm == null) {
            return Collections.emptyMap();
        }
        return store.stream(realm.getId(), offline)
                .flatMap(entity -> entity.getClientSessions().keySet().stream())
                .collect(Collectors.groupingBy(clientId -> clientId, Collectors.counting()));
    }

    @Override
    public void removeUserSession(RealmModel realm, UserSessionModel sessionModel) {
        if (realm == null || sessionModel == null) {
            return;
        }
        store.delete(realm.getId(), sessionModel.getId(), sessionModel.isOffline());
    }

    @Override
    public void removeUserSessions(RealmModel realm, UserModel user) {
        if (realm == null || user == null) {
            return;
        }
        String realmId = realm.getId();
        String userId = user.getId();
        store.removeMatching(realmId, false, entity -> userId.equals(entity.getUserId()));
        store.removeMatching(realmId, true, entity -> userId.equals(entity.getUserId()));
    }

    @Override
    public void removeAllExpired() {
        // TTL enforced by Valkey.
    }

    @Override
    public void removeExpired(RealmModel realm) {
        // TTL enforced by Valkey.
    }

    @Override
    public void removeUserSessions(RealmModel realm) {
        if (realm == null) {
            return;
        }
        store.deleteAllForRealm(realm.getId());
    }

    @Override
    public void onRealmRemoved(RealmModel realm) {
        if (realm == null) {
            return;
        }
        store.deleteAllForRealm(realm.getId());
    }

    @Override
    public void onClientRemoved(RealmModel realm, ClientModel client) {
        if (realm == null || client == null) {
            return;
        }
        String clientId = client.getId();
        store.stream(realm.getId(), false).forEach(entity -> {
            if (entity.getClientSessions().remove(clientId) != null) {
                store.update(realm, entity.getId(), false, existing -> {
                    existing.getClientSessions().remove(clientId);
                    return existing;
                });
            }
        });
        store.stream(realm.getId(), true).forEach(entity -> {
            if (entity.getClientSessions().remove(clientId) != null) {
                store.update(realm, entity.getId(), true, existing -> {
                    existing.getClientSessions().remove(clientId);
                    return existing;
                });
            }
        });
    }

    @Override
    public UserSessionModel createOfflineUserSession(UserSessionModel userSession) {
        Objects.requireNonNull(userSession, "userSession");
        ValkeyUserSessionEntity entity = ValkeyUserSessionEntity.fromModel(userSession);
        entity.setOffline(true);
        entity.setClientSessions(new ConcurrentHashMap<>());
        ValkeyUserSessionEntity persisted = store.create(userSession.getRealm(), entity);
        if (persisted == null) {
            throw new IllegalStateException("Failed to persist Valkey offline user session");
        }
        return new ValkeyUserSessionAdapter(session, this, userSession.getRealm(), persisted, UserSessionModel.SessionPersistenceState.PERSISTENT);
    }

    @Override
    public UserSessionModel getOfflineUserSession(RealmModel realm, String userSessionId) {
        if (realm == null || userSessionId == null) {
            return null;
        }
        return store.load(realm.getId(), userSessionId, true)
                .map(entity -> new ValkeyUserSessionAdapter(session, this, realm, entity, UserSessionModel.SessionPersistenceState.PERSISTENT))
                .orElse(null);
    }

    @Override
    public void removeOfflineUserSession(RealmModel realm, UserSessionModel userSession) {
        if (realm == null || userSession == null) {
            return;
        }
        store.delete(realm.getId(), userSession.getId(), true);
    }

    @Override
    public AuthenticatedClientSessionModel createOfflineClientSession(AuthenticatedClientSessionModel clientSession, UserSessionModel offlineUserSession) {
        Objects.requireNonNull(clientSession, "clientSession");
        Objects.requireNonNull(offlineUserSession, "offlineUserSession");
        RealmModel realm = offlineUserSession.getRealm();
        ClientModel client = clientSession.getClient();
        ValkeyAuthenticatedClientSessionEntity entity = ValkeyAuthenticatedClientSessionEntity.fromModel(clientSession);
        entity.setRealmId(realm.getId());
        entity.setClientId(client.getId());
        entity.setUserSessionId(offlineUserSession.getId());
        ValkeyUserSessionAdapter owner = offlineUserSession instanceof ValkeyUserSessionAdapter valkeyAdapter
                ? valkeyAdapter : toAdapter(realm, offlineUserSession.getId(), true);
        ValkeyAuthenticatedClientSessionEntity persisted = updateClientSession(offlineUserSession, client.getId(), true,
                existing -> entity.copy());
        if (persisted == null) {
            throw new IllegalStateException("Failed to persist Valkey offline client session");
        }
        if (owner != null) {
            store.load(realm.getId(), offlineUserSession.getId(), true).ifPresent(owner::refreshFromStore);
        }
        return new ValkeyAuthenticatedClientSessionAdapter(this, realm, client,
                owner != null ? owner : toAdapter(realm, offlineUserSession.getId(), true), true, persisted.copy());
    }

    @Override
    public Stream<UserSessionModel> getOfflineUserSessionsStream(RealmModel realm, UserModel user) {
        if (realm == null || user == null) {
            return Stream.empty();
        }
        String userId = user.getId();
        return store.stream(realm.getId(), true)
                .filter(entity -> userId.equals(entity.getUserId()))
                .map(entity -> new ValkeyUserSessionAdapter(session, this, realm, entity, UserSessionModel.SessionPersistenceState.PERSISTENT));
    }

    @Override
    public Stream<UserSessionModel> getOfflineUserSessionByBrokerUserIdStream(RealmModel realm, String brokerUserId) {
        if (realm == null || brokerUserId == null) {
            return Stream.empty();
        }
        return store.stream(realm.getId(), true)
                .filter(entity -> brokerUserId.equals(entity.getBrokerUserId()))
                .map(entity -> new ValkeyUserSessionAdapter(session, this, realm, entity, UserSessionModel.SessionPersistenceState.PERSISTENT));
    }

    @Override
    public long getOfflineSessionsCount(RealmModel realm, ClientModel client) {
        if (realm == null || client == null) {
            return 0;
        }
        return store.stream(realm.getId(), true)
                .filter(entity -> entity.getClientSessions().containsKey(client.getId()))
                .count();
    }

    @Override
    public Stream<UserSessionModel> getOfflineUserSessionsStream(RealmModel realm, ClientModel client, Integer firstResult, Integer maxResults) {
        if (realm == null || client == null) {
            return Stream.empty();
        }
        Stream<UserSessionModel> stream = store.stream(realm.getId(), true)
                .filter(entity -> entity.getClientSessions().containsKey(client.getId()))
                .map(entity -> new ValkeyUserSessionAdapter(session, this, realm, entity, UserSessionModel.SessionPersistenceState.PERSISTENT));
        if (firstResult != null && firstResult > 0) {
            stream = stream.skip(firstResult);
        }
        if (maxResults != null && maxResults >= 0) {
            stream = stream.limit(maxResults);
        }
        return stream;
    }

    @Override
    public void importUserSessions(Collection<UserSessionModel> persistentUserSessions, boolean offline) {
        if (persistentUserSessions == null || persistentUserSessions.isEmpty()) {
            return;
        }
        persistentUserSessions.forEach(sessionModel -> {
            ValkeyUserSessionEntity entity = ValkeyUserSessionEntity.fromModel(sessionModel);
            entity.setOffline(offline);
            store.create(sessionModel.getRealm(), entity);
        });
    }

    @Override
    public void close() {
        // Connection handled by ValkeyConnectionProvider
    }

    @Override
    public int getStartupTime(RealmModel realm) {
        return startupTime;
    }

    @Override
    public void migrate(String modelVersion) {
        // no-op
    }

    @Override
    public UserSessionModel getUserSessionIfClientExists(RealmModel realm, String userSessionId, boolean offline, String clientUUID) {
        return UserSessionProvider.super.getUserSessionIfClientExists(realm, userSessionId, offline, clientUUID);
    }

    ValkeyUserSessionEntity updateUserSession(RealmModel realm, String id, boolean offline,
            UnaryOperator<ValkeyUserSessionEntity> mutator) {
        ValkeyUserSessionEntity updated = store.update(realm, id, offline, current -> {
            if (current == null) {
                return null;
            }
            ValkeyUserSessionEntity copy = current.copy();
            ValkeyUserSessionEntity result = mutator.apply(copy);
            if (result == null) {
                return null;
            }
            result.setRealmId(realm.getId());
            result.setOffline(offline);
            return result;
        });
        if (updated == null) {
            throw new IllegalStateException("User session " + id + " no longer exists");
        }
        return updated;
    }

    ValkeyAuthenticatedClientSessionEntity updateClientSession(UserSessionModel userSession, String clientId, boolean offline,
            UnaryOperator<ValkeyAuthenticatedClientSessionEntity> mutator) {
        RealmModel realm = userSession.getRealm();
        ValkeyUserSessionEntity updated = store.update(realm, userSession.getId(), offline, current -> {
            if (current == null) {
                return null;
            }
            ValkeyAuthenticatedClientSessionEntity existing = current.getClientSessions().get(clientId);
            ValkeyAuthenticatedClientSessionEntity mutated = mutator.apply(existing == null ? null : existing.copy());
            if (mutated == null) {
                current.getClientSessions().remove(clientId);
            } else {
                mutated.setRealmId(realm.getId());
                mutated.setClientId(clientId);
                mutated.setUserSessionId(userSession.getId());
                current.getClientSessions().put(clientId, mutated);
            }
            return current;
        });
        if (updated == null) {
            return null;
        }
        return updated.getClientSessions().get(clientId);
    }

    void removeClientSession(UserSessionModel userSession, String clientId, boolean offline) {
        RealmModel realm = userSession.getRealm();
        store.update(realm, userSession.getId(), offline, current -> {
            if (current == null) {
                return null;
            }
            current.getClientSessions().remove(clientId);
            return current;
        });
    }

    void refreshUserSessionTtl(RealmModel realm, String id, boolean offline, long refreshMillis) {
        store.touch(realm, id, offline, refreshMillis);
    }

    private ValkeyUserSessionAdapter toAdapter(RealmModel realm, String sessionId, boolean offline) {
        if (realm == null || sessionId == null) {
            return null;
        }
        return store.load(realm.getId(), sessionId, offline)
                .map(entity -> new ValkeyUserSessionAdapter(session, this, realm, entity,
                        UserSessionModel.SessionPersistenceState.PERSISTENT))
                .orElse(null);
    }
}
