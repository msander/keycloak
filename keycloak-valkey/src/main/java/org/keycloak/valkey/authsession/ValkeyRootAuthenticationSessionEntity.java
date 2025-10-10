package org.keycloak.valkey.authsession;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Root authentication session persisted in Valkey.
 */
final class ValkeyRootAuthenticationSessionEntity {

    private String id;
    private String realmId;
    private int timestamp;
    private Map<String, ValkeyAuthenticationSessionEntity> authenticationSessions;

    @SuppressWarnings("unused")
    public ValkeyRootAuthenticationSessionEntity() {
        this.authenticationSessions = new LinkedHashMap<>();
    }

    ValkeyRootAuthenticationSessionEntity(String id, String realmId, int timestamp) {
        this.id = Objects.requireNonNull(id, "id");
        this.realmId = Objects.requireNonNull(realmId, "realmId");
        this.timestamp = timestamp;
        this.authenticationSessions = new LinkedHashMap<>();
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
        this.realmId = Objects.requireNonNull(realmId, "realmId");
    }

    public int getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, ValkeyAuthenticationSessionEntity> getAuthenticationSessions() {
        return authenticationSessions;
    }

    public void setAuthenticationSessions(Map<String, ValkeyAuthenticationSessionEntity> authenticationSessions) {
        this.authenticationSessions = new LinkedHashMap<>(Objects.requireNonNull(authenticationSessions, "authenticationSessions"));
    }

    ValkeyRootAuthenticationSessionEntity copy() {
        ValkeyRootAuthenticationSessionEntity copy = new ValkeyRootAuthenticationSessionEntity(id, realmId, timestamp);
        Map<String, ValkeyAuthenticationSessionEntity> sessionCopies = new LinkedHashMap<>();
        authenticationSessions.forEach((tabId, session) -> sessionCopies.put(tabId, session.copy()));
        copy.authenticationSessions = sessionCopies;
        return copy;
    }
}
