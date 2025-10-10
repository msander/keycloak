package org.keycloak.valkey.usersession;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.keycloak.models.session.PersistentClientSessionModel;
import org.keycloak.models.session.PersistentUserSessionModel;
import org.keycloak.util.JsonSerialization;

final class ValkeyPersistentModels {

    private ValkeyPersistentModels() {
    }

    static PersistentUserSessionModel userSessionModel(ValkeyUserSessionEntity entity, boolean offline) {
        Objects.requireNonNull(entity, "entity");
        ValkeyPersistentUserSessionModel model = new ValkeyPersistentUserSessionModel();
        model.setUserSessionId(entity.getId());
        model.setStarted(entity.getStarted());
        model.setLastSessionRefresh(entity.getLastSessionRefresh());
        model.setOffline(offline);
        model.setRealmId(entity.getRealmId());
        model.setUserId(entity.getUserId());
        model.setBrokerSessionId(entity.getBrokerSessionId());
        model.setData(writeUserData(entity));
        return model;
    }

    static PersistentClientSessionModel clientSessionModel(ValkeyAuthenticatedClientSessionEntity entity, boolean offline) {
        Objects.requireNonNull(entity, "entity");
        ValkeyPersistentClientSessionModel model = new ValkeyPersistentClientSessionModel();
        model.setUserSessionId(entity.getUserSessionId());
        model.setClientId(entity.getClientId());
        model.setTimestamp(entity.getTimestamp());
        model.setData(writeClientData(entity, offline));
        return model;
    }

    private static String writeUserData(ValkeyUserSessionEntity entity) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("brokerSessionId", entity.getBrokerSessionId());
        payload.put("brokerUserId", entity.getBrokerUserId());
        payload.put("ipAddress", entity.getIpAddress());
        payload.put("authMethod", entity.getAuthMethod());
        payload.put("rememberMe", entity.isRememberMe());
        payload.put("notes", entity.getNotes());
        payload.put("state", entity.getState() == null ? null : entity.getState().name());
        payload.put("loginUsername", entity.getLoginUsername());
        try {
            return JsonSerialization.writeValueAsString(payload);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to encode persistent user session payload", ex);
        }
    }

    private static String writeClientData(ValkeyAuthenticatedClientSessionEntity entity, boolean offline) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("authMethod", entity.getAuthMethod());
        payload.put("redirectUri", entity.getRedirectUri());
        payload.put("notes", entity.getNotes());
        payload.put("action", entity.getAction());
        if (offline) {
            payload.put("userSessionNotes", entity.getNotes());
        }
        try {
            return JsonSerialization.writeValueAsString(payload);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to encode persistent client session payload", ex);
        }
    }
}
