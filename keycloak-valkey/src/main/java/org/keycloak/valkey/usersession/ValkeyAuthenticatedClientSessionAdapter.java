package org.keycloak.valkey.usersession;

import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;

import org.keycloak.common.util.Time;
import org.keycloak.models.AuthenticatedClientSessionModel;
import org.keycloak.models.ClientModel;
import org.keycloak.models.RealmModel;
import org.keycloak.models.UserSessionModel;

final class ValkeyAuthenticatedClientSessionAdapter implements AuthenticatedClientSessionModel {

    private final ValkeyUserSessionProvider provider;
    private final RealmModel realm;
    private final ClientModel client;
    private final UserSessionModel userSession;
    private final boolean offline;
    private final String clientId;
    private final String id;

    private ValkeyAuthenticatedClientSessionEntity entity;

    ValkeyAuthenticatedClientSessionAdapter(ValkeyUserSessionProvider provider, RealmModel realm, ClientModel client,
            UserSessionModel userSession, boolean offline, ValkeyAuthenticatedClientSessionEntity entity) {
        this.provider = Objects.requireNonNull(provider, "provider");
        this.realm = Objects.requireNonNull(realm, "realm");
        this.client = Objects.requireNonNull(client, "client");
        this.userSession = Objects.requireNonNull(userSession, "userSession");
        this.offline = offline;
        this.clientId = client.getId();
        this.id = entity.getUserSessionId() + "::" + this.clientId;
        this.entity = Objects.requireNonNull(entity, "entity");
    }

    @Override
    public void detachFromUserSession() {
        provider.removeClientSession(userSession, clientId, offline);
    }

    @Override
    public UserSessionModel getUserSession() {
        return userSession;
    }

    @Override
    public String getRedirectUri() {
        return entity.getRedirectUri();
    }

    @Override
    public void setRedirectUri(String uri) {
        mutate(session -> {
            session.setRedirectUri(uri);
            return session;
        });
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public RealmModel getRealm() {
        return realm;
    }

    @Override
    public ClientModel getClient() {
        return client;
    }

    @Override
    public int getTimestamp() {
        return entity.getTimestamp();
    }

    @Override
    public void setTimestamp(int timestamp) {
        if (timestamp <= entity.getTimestamp()) {
            return;
        }
        mutate(session -> {
            if (session.getTimestamp() >= timestamp) {
                return session;
            }
            session.setTimestamp(timestamp);
            return session;
        });
    }

    @Override
    public String getAction() {
        return entity.getAction();
    }

    @Override
    public void setAction(String action) {
        mutate(session -> {
            session.setAction(action);
            return session;
        });
    }

    @Override
    public String getProtocol() {
        return entity.getAuthMethod();
    }

    @Override
    public void setProtocol(String method) {
        mutate(session -> {
            session.setAuthMethod(method);
            return session;
        });
    }

    @Override
    public String getNote(String name) {
        return entity.getNotes().get(name);
    }

    @Override
    public void setNote(String name, String value) {
        mutate(session -> {
            session.getNotes().put(name, value);
            return session;
        });
    }

    @Override
    public void removeNote(String name) {
        mutate(session -> {
            session.getNotes().remove(name);
            return session;
        });
    }

    @Override
    public Map<String, String> getNotes() {
        return entity.getNotesView();
    }

    @Override
    public void restartClientSession() {
        mutate(session -> {
            session.setAction(null);
            session.setRedirectUri(null);
            session.setTimestamp(Time.currentTime());
            session.getNotes().keySet().removeIf(note -> !AuthenticatedClientSessionModel.STARTED_AT_NOTE.equals(note)
                    && !AuthenticatedClientSessionModel.USER_SESSION_STARTED_AT_NOTE.equals(note)
                    && !AuthenticatedClientSessionModel.USER_SESSION_REMEMBER_ME_NOTE.equals(note));
            session.getNotes().put(AuthenticatedClientSessionModel.STARTED_AT_NOTE, String.valueOf(session.getTimestamp()));
            return session;
        });
    }

    private void mutate(UnaryOperator<ValkeyAuthenticatedClientSessionEntity> mutator) {
        ValkeyAuthenticatedClientSessionEntity updated = provider.updateClientSession(userSession, clientId, offline, mutator);
        if (updated != null) {
            entity = updated;
        }
    }
}
