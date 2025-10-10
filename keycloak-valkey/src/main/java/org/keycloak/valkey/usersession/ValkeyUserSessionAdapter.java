package org.keycloak.valkey.usersession;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;

import org.keycloak.common.util.Time;
import org.keycloak.models.AuthenticatedClientSessionModel;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.models.UserModel;
import org.keycloak.models.UserSessionModel;

final class ValkeyUserSessionAdapter implements UserSessionModel {

    private final KeycloakSession session;
    private final ValkeyUserSessionProvider provider;
    private final RealmModel realm;
    private final boolean offline;
    private final SessionPersistenceState persistenceState;

    private ValkeyUserSessionEntity entity;

    ValkeyUserSessionAdapter(KeycloakSession session, ValkeyUserSessionProvider provider, RealmModel realm,
            ValkeyUserSessionEntity entity, SessionPersistenceState persistenceState) {
        this.session = Objects.requireNonNull(session, "session");
        this.provider = Objects.requireNonNull(provider, "provider");
        this.realm = Objects.requireNonNull(realm, "realm");
        this.entity = Objects.requireNonNull(entity, "entity");
        this.offline = entity.isOffline();
        this.persistenceState = persistenceState;
    }

    @Override
    public String getId() {
        return entity.getId();
    }

    @Override
    public RealmModel getRealm() {
        return realm;
    }

    @Override
    public String getBrokerSessionId() {
        return entity.getBrokerSessionId();
    }

    @Override
    public String getBrokerUserId() {
        return entity.getBrokerUserId();
    }

    @Override
    public UserModel getUser() {
        return session.users().getUserById(realm, entity.getUserId());
    }

    @Override
    public String getLoginUsername() {
        return entity.getLoginUsername();
    }

    @Override
    public String getIpAddress() {
        return entity.getIpAddress();
    }

    @Override
    public String getAuthMethod() {
        return entity.getAuthMethod();
    }

    @Override
    public boolean isRememberMe() {
        return entity.isRememberMe();
    }

    @Override
    public int getStarted() {
        return entity.getStarted();
    }

    @Override
    public int getLastSessionRefresh() {
        return entity.getLastSessionRefresh();
    }

    @Override
    public void setLastSessionRefresh(int seconds) {
        if (seconds <= entity.getLastSessionRefresh()) {
            return;
        }
        mutate(sessionEntity -> {
            sessionEntity.setLastSessionRefresh(seconds);
            return sessionEntity;
        });
    }

    @Override
    public boolean isOffline() {
        return offline;
    }

    @Override
    public Map<String, AuthenticatedClientSessionModel> getAuthenticatedClientSessions() {
        Map<String, AuthenticatedClientSessionModel> result = new LinkedHashMap<>();
        entity.getClientSessions().forEach((clientId, sessionEntity) -> {
            var client = realm.getClientById(clientId);
            if (client != null) {
                result.put(clientId, new ValkeyAuthenticatedClientSessionAdapter(provider, realm, client, this, offline,
                        sessionEntity.copy()));
            }
        });
        return result;
    }

    @Override
    public void removeAuthenticatedClientSessions(Collection<String> removedClientUUIDS) {
        if (removedClientUUIDS == null || removedClientUUIDS.isEmpty()) {
            return;
        }
        mutate(entity -> {
            removedClientUUIDS.forEach(entity.getClientSessions()::remove);
            return entity;
        });
    }

    @Override
    public String getNote(String name) {
        return entity.getNotes().get(name);
    }

    @Override
    public void setNote(String name, String value) {
        mutate(entity -> {
            entity.getNotes().put(name, value);
            return entity;
        });
    }

    @Override
    public void removeNote(String name) {
        mutate(entity -> {
            entity.getNotes().remove(name);
            return entity;
        });
    }

    @Override
    public Map<String, String> getNotes() {
        return entity.getNotesView();
    }

    @Override
    public State getState() {
        return entity.getState();
    }

    @Override
    public void setState(State state) {
        mutate(entity -> {
            entity.setState(state);
            return entity;
        });
    }

    @Override
    public void restartSession(RealmModel realm, UserModel user, String loginUsername, String ipAddress, String authMethod,
            boolean rememberMe, String brokerSessionId, String brokerUserId) {
        mutate(entity -> {
            entity.setRealmId(realm.getId());
            entity.setUserId(user.getId());
            entity.setLoginUsername(loginUsername);
            entity.setIpAddress(ipAddress);
            entity.setAuthMethod(authMethod);
            entity.setRememberMe(rememberMe);
            entity.setBrokerSessionId(brokerSessionId);
            entity.setBrokerUserId(brokerUserId);
            int now = Time.currentTime();
            entity.setStarted(now);
            entity.setLastSessionRefresh(now);
            entity.setState(State.LOGGED_IN);
            entity.getNotes().clear();
            entity.getClientSessions().clear();
            return entity;
        });
    }

    @Override
    public SessionPersistenceState getPersistenceState() {
        return persistenceState;
    }

    void refreshFromStore(ValkeyUserSessionEntity reloaded) {
        this.entity = Objects.requireNonNull(reloaded, "reloaded");
    }

    ValkeyUserSessionEntity getEntity() {
        return entity;
    }

    private void mutate(UnaryOperator<ValkeyUserSessionEntity> mutator) {
        if (persistenceState == SessionPersistenceState.TRANSIENT) {
            entity = mutator.apply(entity);
            return;
        }
        entity = provider.updateUserSession(realm, entity.getId(), offline, mutator);
    }
}
