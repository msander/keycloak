package org.keycloak.valkey.usersession;

import org.keycloak.models.session.PersistentClientSessionModel;

final class ValkeyPersistentClientSessionModel implements PersistentClientSessionModel {

    private String userSessionId;
    private String clientId;
    private int timestamp;
    private String data;

    @Override
    public String getUserSessionId() {
        return userSessionId;
    }

    @Override
    public void setUserSessionId(String userSessionId) {
        this.userSessionId = userSessionId;
    }

    @Override
    public String getClientId() {
        return clientId;
    }

    @Override
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    @Override
    public int getTimestamp() {
        return timestamp;
    }

    @Override
    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String getData() {
        return data;
    }

    @Override
    public void setData(String data) {
        this.data = data;
    }
}
