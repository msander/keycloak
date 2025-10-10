package org.keycloak.valkey.authsession;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.keycloak.sessions.AuthenticationSessionModel;

/**
 * Mutable representation of an authentication session stored in Valkey.
 */
final class ValkeyAuthenticationSessionEntity {

    private String clientUUID;
    private String authUserId;
    private int timestamp;
    private String redirectUri;
    private String action;
    private Set<String> clientScopes;
    private Map<String, AuthenticationSessionModel.ExecutionStatus> executionStatus;
    private String protocol;
    private Map<String, String> clientNotes;
    private Map<String, String> authNotes;
    private Set<String> requiredActions;
    private Map<String, String> userSessionNotes;

    @SuppressWarnings("unused")
    public ValkeyAuthenticationSessionEntity() {
        this.clientScopes = new HashSet<>();
        this.executionStatus = new HashMap<>();
        this.clientNotes = new HashMap<>();
        this.authNotes = new HashMap<>();
        this.requiredActions = new HashSet<>();
        this.userSessionNotes = new HashMap<>();
    }

    ValkeyAuthenticationSessionEntity copy() {
        ValkeyAuthenticationSessionEntity copy = new ValkeyAuthenticationSessionEntity();
        copy.clientUUID = clientUUID;
        copy.authUserId = authUserId;
        copy.timestamp = timestamp;
        copy.redirectUri = redirectUri;
        copy.action = action;
        copy.clientScopes = new HashSet<>(clientScopes);
        copy.executionStatus = new HashMap<>(executionStatus);
        copy.protocol = protocol;
        copy.clientNotes = new HashMap<>(clientNotes);
        copy.authNotes = new HashMap<>(authNotes);
        copy.requiredActions = new HashSet<>(requiredActions);
        copy.userSessionNotes = new HashMap<>(userSessionNotes);
        return copy;
    }

    static ValkeyAuthenticationSessionEntity forClient(String clientId, int timestamp) {
        ValkeyAuthenticationSessionEntity entity = new ValkeyAuthenticationSessionEntity();
        entity.clientUUID = Objects.requireNonNull(clientId, "clientId");
        entity.timestamp = timestamp;
        return entity;
    }

    public String getClientUUID() {
        return clientUUID;
    }

    public void setClientUUID(String clientUUID) {
        this.clientUUID = clientUUID;
    }

    public String getAuthUserId() {
        return authUserId;
    }

    public void setAuthUserId(String authUserId) {
        this.authUserId = authUserId;
    }

    public int getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }

    public String getRedirectUri() {
        return redirectUri;
    }

    public void setRedirectUri(String redirectUri) {
        this.redirectUri = redirectUri;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Set<String> getClientScopes() {
        return clientScopes;
    }

    public void setClientScopes(Set<String> clientScopes) {
        this.clientScopes = Objects.requireNonNullElseGet(clientScopes, HashSet::new);
    }

    public Map<String, AuthenticationSessionModel.ExecutionStatus> getExecutionStatus() {
        return executionStatus;
    }

    public void setExecutionStatus(Map<String, AuthenticationSessionModel.ExecutionStatus> executionStatus) {
        this.executionStatus = Objects.requireNonNullElseGet(executionStatus, HashMap::new);
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public Map<String, String> getClientNotes() {
        return clientNotes;
    }

    public void setClientNotes(Map<String, String> clientNotes) {
        this.clientNotes = Objects.requireNonNullElseGet(clientNotes, HashMap::new);
    }

    public Map<String, String> getAuthNotes() {
        return authNotes;
    }

    public void setAuthNotes(Map<String, String> authNotes) {
        this.authNotes = Objects.requireNonNullElseGet(authNotes, HashMap::new);
    }

    public Set<String> getRequiredActions() {
        return requiredActions;
    }

    public void setRequiredActions(Set<String> requiredActions) {
        this.requiredActions = Objects.requireNonNullElseGet(requiredActions, HashSet::new);
    }

    public Map<String, String> getUserSessionNotes() {
        return userSessionNotes;
    }

    public void setUserSessionNotes(Map<String, String> userSessionNotes) {
        this.userSessionNotes = Objects.requireNonNullElseGet(userSessionNotes, HashMap::new);
    }
}
