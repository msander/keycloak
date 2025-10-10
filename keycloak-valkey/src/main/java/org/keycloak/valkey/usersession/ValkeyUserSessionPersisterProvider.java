package org.keycloak.valkey.usersession;

import static java.util.stream.Collectors.toMap;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import org.jboss.logging.Logger;
import org.keycloak.models.AuthenticatedClientSessionModel;
import org.keycloak.models.ClientModel;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.models.UserModel;
import org.keycloak.models.UserSessionModel;
import org.keycloak.models.session.PersistentAuthenticatedClientSessionAdapter;
import org.keycloak.models.session.PersistentClientSessionModel;
import org.keycloak.models.session.PersistentUserSessionAdapter;
import org.keycloak.models.session.PersistentUserSessionModel;
import org.keycloak.models.session.UserSessionPersisterProvider;
import org.keycloak.models.utils.SessionExpirationUtils;
import org.keycloak.valkey.ValkeyConnectionProvider;

final class ValkeyUserSessionPersisterProvider implements UserSessionPersisterProvider {

    private static final Logger logger = Logger.getLogger(ValkeyUserSessionPersisterProvider.class);

    private final KeycloakSession session;
    private final ValkeyUserSessionPersisterStore store;

    ValkeyUserSessionPersisterProvider(KeycloakSession session, ValkeyConnectionProvider connectionProvider) {
        this.session = Objects.requireNonNull(session, "session");
        ValkeyConnectionProvider provider = Objects.requireNonNull(connectionProvider, "connectionProvider");
        this.store = new ValkeyUserSessionPersisterStore(provider.getConnection().sync(), "valkey-persister");
    }

    @Override
    public void createUserSession(UserSessionModel userSession, boolean offline) {
        Objects.requireNonNull(userSession, "userSession");
        ValkeyUserSessionEntity entity = ValkeyUserSessionEntity.fromModel(userSession);
        entity.setOffline(offline);
        store.save(entity);
    }

    @Override
    public void createClientSession(AuthenticatedClientSessionModel clientSession, boolean offline) {
        Objects.requireNonNull(clientSession, "clientSession");
        update(clientSession.getUserSession().getId(), offline, current -> {
            if (current == null) {
                return null;
            }
            ValkeyAuthenticatedClientSessionEntity clientEntity = ValkeyAuthenticatedClientSessionEntity.fromModel(clientSession);
            current.getClientSessions().put(clientEntity.getClientId(), clientEntity);
            return current;
        });
    }

    @Override
    public void removeUserSession(String userSessionId, boolean offline) {
        if (userSessionId == null) {
            return;
        }
        store.delete(userSessionId, offline);
    }

    @Override
    public void removeClientSession(String userSessionId, String clientUUID, boolean offline) {
        if (userSessionId == null || clientUUID == null) {
            return;
        }
        update(userSessionId, offline, current -> {
            if (current == null) {
                return null;
            }
            current.getClientSessions().remove(clientUUID);
            return current;
        });
    }

    @Override
    public void onRealmRemoved(RealmModel realm) {
        if (realm == null) {
            return;
        }
        store.deleteRealm(realm.getId());
    }

    @Override
    public void onClientRemoved(RealmModel realm, ClientModel client) {
        if (realm == null || client == null) {
            return;
        }
        store.removeClientSessions(realm.getId(), client.getId());
    }

    @Override
    public void onUserRemoved(RealmModel realm, UserModel user) {
        if (realm == null || user == null) {
            return;
        }
        store.removeUserSessions(realm.getId(), user.getId());
    }

    @Override
    public void updateLastSessionRefreshes(RealmModel realm, int lastSessionRefresh, Collection<String> userSessionIds,
            boolean offline) {
        if (realm == null || userSessionIds == null || userSessionIds.isEmpty()) {
            return;
        }
        for (String id : userSessionIds) {
            update(id, offline, current -> {
                if (current == null) {
                    return null;
                }
                current.setLastSessionRefresh(lastSessionRefresh);
                return current;
            });
        }
    }

    @Override
    public void removeExpired(RealmModel realm) {
        if (realm == null) {
            return;
        }
        long now = System.currentTimeMillis();
        store.streamRealmSessions(realm.getId())
                .filter(entity -> isExpired(realm, entity, now))
                .forEach(entity -> store.delete(entity.getId(), entity.isOffline()));
    }

    @Override
    public UserSessionModel loadUserSession(RealmModel realm, String userSessionId, boolean offline) {
        if (realm == null || userSessionId == null) {
            return null;
        }
        return store.load(userSessionId, offline)
                .filter(entity -> realm.getId().equals(entity.getRealmId()))
                .map(entity -> toAdapter(entity, offline))
                .orElse(null);
    }

    @Override
    public Stream<UserSessionModel> loadUserSessionsStream(RealmModel realm, UserModel user, boolean offline, Integer firstResult,
            Integer maxResults) {
        if (realm == null || user == null) {
            return Stream.empty();
        }
        return store.streamUserSessions(realm.getId(), user.getId(), offline, firstResult, maxResults)
                .map(entity -> toAdapter(entity, offline));
    }

    @Override
    public Stream<UserSessionModel> loadUserSessionsStream(RealmModel realm, ClientModel client, boolean offline, Integer firstResult,
            Integer maxResults) {
        if (realm == null || client == null) {
            return Stream.empty();
        }
        return store.streamClientSessions(realm.getId(), client.getId(), offline, firstResult, maxResults)
                .map(entity -> toAdapter(entity, offline));
    }

    @Override
    public UserSessionModel loadUserSessionsStreamByBrokerSessionId(RealmModel realm, String brokerSessionId, boolean offline) {
        if (realm == null || brokerSessionId == null) {
            return null;
        }
        return store.streamRealmSessions(realm.getId())
                .filter(entity -> offline == entity.isOffline())
                .filter(entity -> brokerSessionId.equals(entity.getBrokerSessionId()))
                .map(entity -> toAdapter(entity, offline))
                .findFirst()
                .orElse(null);
    }

    @Override
    public Stream<UserSessionModel> loadUserSessionsStream(Integer firstResult, Integer maxResults, boolean offline,
            String lastUserSessionId) {
        return store.streamAll(offline, firstResult, maxResults, lastUserSessionId)
                .map(entity -> toAdapter(entity, offline));
    }

    @Override
    public AuthenticatedClientSessionModel loadClientSession(RealmModel realm, ClientModel client, UserSessionModel userSession,
            boolean offline) {
        if (realm == null || client == null || userSession == null) {
            return null;
        }
        return store.load(userSession.getId(), offline)
                .map(entity -> entity.getClientSessions().get(client.getId()))
                .map(clientEntity -> toClientAdapter(clientEntity, realm, client, userSession, offline))
                .orElse(null);
    }

    @Override
    public int getUserSessionsCount(boolean offline) {
        return store.countAll(offline);
    }

    @Override
    public int getUserSessionsCount(RealmModel realm, ClientModel clientModel, boolean offline) {
        if (realm == null || clientModel == null) {
            return 0;
        }
        return store.countClientSessions(realm.getId(), clientModel.getId(), offline);
    }

    @Override
    public Map<String, Long> getUserSessionsCountsByClients(RealmModel realm, boolean offline) {
        if (realm == null) {
            return Collections.emptyMap();
        }
        return store.countSessionsByClient(realm.getId(), offline);
    }

    @Override
    public void removeUserSessions(RealmModel realm, boolean offline) {
        if (realm == null) {
            return;
        }
        store.removeRealmSessions(realm.getId(), offline);
    }

    @Override
    public void close() {
        // no-op
    }

    private void update(String sessionId, boolean offline, UnaryOperator<ValkeyUserSessionEntity> mutator) {
        if (sessionId == null) {
            return;
        }
        try {
            store.update(sessionId, offline, mutator);
        } catch (RuntimeException ex) {
            logger.warnf(ex, "Failed to update persistent user session %s", sessionId);
        }
    }

    private boolean isExpired(RealmModel realm, ValkeyUserSessionEntity entity, long nowMillis) {
        long createdMillis = entity.getStarted() * 1000L;
        long idleMillis = entity.getLastSessionRefresh() * 1000L;
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
        return expiry > 0 && expiry <= nowMillis;
    }

    private UserSessionModel toAdapter(ValkeyUserSessionEntity entity, boolean offline) {
        RealmModel realm = session.realms().getRealm(entity.getRealmId());
        if (realm == null) {
            return null;
        }
        PersistentUserSessionModel model = ValkeyPersistentModels.userSessionModel(entity, offline);
        Map<String, AuthenticatedClientSessionModel> clientSessions = new ConcurrentHashMap<>();
        PersistentUserSessionAdapter adapter = new PersistentUserSessionAdapter(session, model, realm, entity.getUserId(), clientSessions);
        adapter.setClientSessionsLoader(target -> target.putAll(loadClientSessions(entity, adapter, realm)));
        return adapter;
    }

    private Map<String, AuthenticatedClientSessionModel> loadClientSessions(ValkeyUserSessionEntity entity,
            UserSessionModel parent, RealmModel realm) {
        return entity.getClientSessions().values().stream()
                .map(clientEntity -> toClientAdapter(clientEntity, realm, realm.getClientById(clientEntity.getClientId()), parent, entity.isOffline()))
                .filter(Objects::nonNull)
                .collect(toMap(client -> client.getClient().getId(), client -> client));
    }

    private AuthenticatedClientSessionModel toClientAdapter(ValkeyAuthenticatedClientSessionEntity entity, RealmModel realm,
            ClientModel client, UserSessionModel userSession, boolean offline) {
        if (entity == null || client == null) {
            return null;
        }
        PersistentClientSessionModel model = ValkeyPersistentModels.clientSessionModel(entity, offline);
        PersistentAuthenticatedClientSessionAdapter adapter = new PersistentAuthenticatedClientSessionAdapter(session, model, realm, client, userSession);
        return adapter;
    }
}
