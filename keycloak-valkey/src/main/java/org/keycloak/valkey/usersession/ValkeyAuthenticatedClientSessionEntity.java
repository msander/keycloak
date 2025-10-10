package org.keycloak.valkey.usersession;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.keycloak.models.AuthenticatedClientSessionModel;
import org.keycloak.models.ClientModel;
import org.keycloak.models.RealmModel;
import org.keycloak.models.UserSessionModel;

/**
 * Serializable representation of an authenticated client session stored in Valkey.
 */
final class ValkeyAuthenticatedClientSessionEntity {

    private String realmId;
    private String clientId;
    private String userSessionId;
    private String authMethod;
    private String redirectUri;
    private int timestamp;
    private String action;
    private Map<String, String> notes = new HashMap<>();

    @SuppressWarnings("unused")
    public ValkeyAuthenticatedClientSessionEntity() {
    }

    static ValkeyAuthenticatedClientSessionEntity create(RealmModel realm, ClientModel client, UserSessionModel userSession) {
        Objects.requireNonNull(realm, "realm");
        Objects.requireNonNull(client, "client");
        Objects.requireNonNull(userSession, "userSession");

        ValkeyAuthenticatedClientSessionEntity entity = new ValkeyAuthenticatedClientSessionEntity();
        entity.realmId = realm.getId();
        entity.clientId = client.getId();
        entity.userSessionId = userSession.getId();
        entity.timestamp = org.keycloak.common.util.Time.currentTime();
        entity.notes.put(AuthenticatedClientSessionModel.STARTED_AT_NOTE, String.valueOf(entity.timestamp));
        entity.notes.put(AuthenticatedClientSessionModel.USER_SESSION_STARTED_AT_NOTE, String.valueOf(userSession.getStarted()));
        if (userSession.isRememberMe()) {
            entity.notes.put(AuthenticatedClientSessionModel.USER_SESSION_REMEMBER_ME_NOTE, Boolean.TRUE.toString());
        }
        return entity;
    }

    static ValkeyAuthenticatedClientSessionEntity fromModel(AuthenticatedClientSessionModel model) {
        Objects.requireNonNull(model, "model");
        ValkeyAuthenticatedClientSessionEntity entity = create(model.getRealm(), model.getClient(), model.getUserSession());
        entity.authMethod = model.getProtocol();
        entity.redirectUri = model.getRedirectUri();
        entity.timestamp = model.getTimestamp();
        entity.action = model.getAction();
        entity.notes = new HashMap<>(model.getNotes());
        return entity;
    }

    ValkeyAuthenticatedClientSessionEntity copy() {
        ValkeyAuthenticatedClientSessionEntity copy = new ValkeyAuthenticatedClientSessionEntity();
        copy.realmId = realmId;
        copy.clientId = clientId;
        copy.userSessionId = userSessionId;
        copy.authMethod = authMethod;
        copy.redirectUri = redirectUri;
        copy.timestamp = timestamp;
        copy.action = action;
        copy.notes = new HashMap<>(notes);
        return copy;
    }

    public String getRealmId() {
        return realmId;
    }

    public void setRealmId(String realmId) {
        this.realmId = realmId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getUserSessionId() {
        return userSessionId;
    }

    public void setUserSessionId(String userSessionId) {
        this.userSessionId = userSessionId;
    }

    public String getAuthMethod() {
        return authMethod;
    }

    public void setAuthMethod(String authMethod) {
        this.authMethod = authMethod;
    }

    public String getRedirectUri() {
        return redirectUri;
    }

    public void setRedirectUri(String redirectUri) {
        this.redirectUri = redirectUri;
    }

    public int getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Map<String, String> getNotes() {
        return notes;
    }

    public void setNotes(Map<String, String> notes) {
        this.notes = notes == null ? new HashMap<>() : new HashMap<>(notes);
    }

    @JsonIgnore
    public Map<String, String> getNotesView() {
        return Collections.unmodifiableMap(notes);
    }
}
