package org.keycloak.valkey.usersession;

import org.keycloak.models.session.PersistentUserSessionModel;

final class ValkeyPersistentUserSessionModel implements PersistentUserSessionModel {

    private String userSessionId;
    private int started;
    private int lastSessionRefresh;
    private boolean offline;
    private String data;
    private String realmId;
    private String userId;
    private String brokerSessionId;

    @Override
    public String getUserSessionId() {
        return userSessionId;
    }

    @Override
    public void setUserSessionId(String userSessionId) {
        this.userSessionId = userSessionId;
    }

    @Override
    public int getStarted() {
        return started;
    }

    @Override
    public void setStarted(int started) {
        this.started = started;
    }

    @Override
    public int getLastSessionRefresh() {
        return lastSessionRefresh;
    }

    @Override
    public void setLastSessionRefresh(int lastSessionRefresh) {
        this.lastSessionRefresh = lastSessionRefresh;
    }

    @Override
    public boolean isOffline() {
        return offline;
    }

    @Override
    public void setOffline(boolean offline) {
        this.offline = offline;
    }

    @Override
    public String getData() {
        return data;
    }

    @Override
    public void setData(String data) {
        this.data = data;
    }

    @Override
    public void setRealmId(String realmId) {
        this.realmId = realmId;
    }

    @Override
    public void setUserId(String userId) {
        this.userId = userId;
    }

    @Override
    public void setBrokerSessionId(String brokerSessionId) {
        this.brokerSessionId = brokerSessionId;
    }

    String getRealmId() {
        return realmId;
    }

    String getUserId() {
        return userId;
    }

    String getBrokerSessionId() {
        return brokerSessionId;
    }
}
