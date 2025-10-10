package org.keycloak.valkey.usersession;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.keycloak.common.util.Time;
import org.keycloak.models.RealmModel;
import org.keycloak.models.UserModel;
import org.keycloak.models.UserSessionModel;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Serializable representation of a user session stored in Valkey.
 */
final class ValkeyUserSessionEntity {

    private String id;
    private String realmId;
    private String userId;
    private String loginUsername;
    private String ipAddress;
    private String authMethod;
    private boolean rememberMe;
    private int started;
    private int lastSessionRefresh;
    private UserSessionModel.State state;
    private String brokerSessionId;
    private String brokerUserId;
    private Map<String, String> notes = new HashMap<>();
    private Map<String, ValkeyAuthenticatedClientSessionEntity> clientSessions = new LinkedHashMap<>();
    private boolean offline;

    @SuppressWarnings("unused")
    public ValkeyUserSessionEntity() {
    }

    static ValkeyUserSessionEntity create(String id, RealmModel realm, UserModel user, String loginUsername,
            String ipAddress, String authMethod, boolean rememberMe, String brokerSessionId, String brokerUserId,
            boolean offline) {
        Objects.requireNonNull(realm, "realm");
        Objects.requireNonNull(user, "user");
        ValkeyUserSessionEntity entity = new ValkeyUserSessionEntity();
        entity.id = id;
        entity.realmId = realm.getId();
        entity.userId = user.getId();
        entity.loginUsername = loginUsername;
        entity.ipAddress = ipAddress;
        entity.authMethod = authMethod;
        entity.rememberMe = rememberMe;
        entity.started = Time.currentTime();
        entity.lastSessionRefresh = entity.started;
        entity.state = UserSessionModel.State.LOGGED_IN;
        entity.brokerSessionId = brokerSessionId;
        entity.brokerUserId = brokerUserId;
        entity.offline = offline;
        return entity;
    }

    static ValkeyUserSessionEntity fromModel(UserSessionModel model) {
        Objects.requireNonNull(model, "model");
        ValkeyUserSessionEntity entity = new ValkeyUserSessionEntity();
        entity.id = model.getId();
        entity.realmId = model.getRealm().getId();
        entity.userId = model.getUser().getId();
        entity.loginUsername = model.getLoginUsername();
        entity.ipAddress = model.getIpAddress();
        entity.authMethod = model.getAuthMethod();
        entity.rememberMe = model.isRememberMe();
        entity.started = model.getStarted();
        entity.lastSessionRefresh = model.getLastSessionRefresh();
        entity.state = model.getState();
        entity.brokerSessionId = model.getBrokerSessionId();
        entity.brokerUserId = model.getBrokerUserId();
        entity.offline = model.isOffline();
        entity.notes = new HashMap<>(model.getNotes());
        model.getAuthenticatedClientSessions().forEach((clientId, session) ->
                entity.clientSessions.put(clientId, ValkeyAuthenticatedClientSessionEntity.fromModel(session)));
        return entity;
    }

    ValkeyUserSessionEntity copy() {
        ValkeyUserSessionEntity copy = new ValkeyUserSessionEntity();
        copy.id = id;
        copy.realmId = realmId;
        copy.userId = userId;
        copy.loginUsername = loginUsername;
        copy.ipAddress = ipAddress;
        copy.authMethod = authMethod;
        copy.rememberMe = rememberMe;
        copy.started = started;
        copy.lastSessionRefresh = lastSessionRefresh;
        copy.state = state;
        copy.brokerSessionId = brokerSessionId;
        copy.brokerUserId = brokerUserId;
        copy.notes = new HashMap<>(notes);
        Map<String, ValkeyAuthenticatedClientSessionEntity> clientCopies = new LinkedHashMap<>();
        clientSessions.forEach((clientId, session) -> clientCopies.put(clientId, session.copy()));
        copy.clientSessions = clientCopies;
        copy.offline = offline;
        return copy;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getRealmId() {
        return realmId;
    }

    public void setRealmId(String realmId) {
        this.realmId = realmId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getLoginUsername() {
        return loginUsername;
    }

    public void setLoginUsername(String loginUsername) {
        this.loginUsername = loginUsername;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getAuthMethod() {
        return authMethod;
    }

    public void setAuthMethod(String authMethod) {
        this.authMethod = authMethod;
    }

    public boolean isRememberMe() {
        return rememberMe;
    }

    public void setRememberMe(boolean rememberMe) {
        this.rememberMe = rememberMe;
    }

    public int getStarted() {
        return started;
    }

    public void setStarted(int started) {
        this.started = started;
    }

    public int getLastSessionRefresh() {
        return lastSessionRefresh;
    }

    public void setLastSessionRefresh(int lastSessionRefresh) {
        this.lastSessionRefresh = lastSessionRefresh;
    }

    public UserSessionModel.State getState() {
        return state;
    }

    public void setState(UserSessionModel.State state) {
        this.state = state;
    }

    public String getBrokerSessionId() {
        return brokerSessionId;
    }

    public void setBrokerSessionId(String brokerSessionId) {
        this.brokerSessionId = brokerSessionId;
    }

    public String getBrokerUserId() {
        return brokerUserId;
    }

    public void setBrokerUserId(String brokerUserId) {
        this.brokerUserId = brokerUserId;
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

    public Map<String, ValkeyAuthenticatedClientSessionEntity> getClientSessions() {
        return clientSessions;
    }

    public void setClientSessions(Map<String, ValkeyAuthenticatedClientSessionEntity> clientSessions) {
        this.clientSessions = clientSessions == null ? new LinkedHashMap<>() : new LinkedHashMap<>(clientSessions);
    }

    public boolean isOffline() {
        return offline;
    }

    public void setOffline(boolean offline) {
        this.offline = offline;
    }
}
