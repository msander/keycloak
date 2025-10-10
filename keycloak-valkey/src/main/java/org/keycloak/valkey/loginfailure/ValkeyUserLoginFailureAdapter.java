package org.keycloak.valkey.loginfailure;

import org.keycloak.models.RealmModel;
import org.keycloak.models.UserLoginFailureModel;

/**
 * Adapter exposing {@link ValkeyLoginFailureRecord} data through the {@link UserLoginFailureModel} contract.
 */
final class ValkeyUserLoginFailureAdapter implements UserLoginFailureModel {

    private final ValkeyUserLoginFailureProvider provider;
    private final RealmModel realm;
    private final String key;
    private ValkeyLoginFailureRecord record;

    ValkeyUserLoginFailureAdapter(ValkeyUserLoginFailureProvider provider, RealmModel realm, String key,
            ValkeyLoginFailureRecord record) {
        this.provider = provider;
        this.realm = realm;
        this.key = key;
        this.record = record;
    }

    @Override
    public String getUserId() {
        return record.getUserId();
    }

    @Override
    public int getFailedLoginNotBefore() {
        return record.getFailedLoginNotBefore();
    }

    @Override
    public void setFailedLoginNotBefore(int notBefore) {
        int updated = provider.updateFailedLoginNotBefore(key, realm, notBefore);
        record = record.withFailedLoginNotBefore(updated);
    }

    @Override
    public int getNumFailures() {
        return record.getNumFailures();
    }

    @Override
    public void incrementFailures() {
        int updated = provider.incrementFailures(key, realm);
        record = record.withNumFailures(updated);
    }

    @Override
    public int getNumTemporaryLockouts() {
        return record.getNumTemporaryLockouts();
    }

    @Override
    public void incrementTemporaryLockouts() {
        int updated = provider.incrementTemporaryLockouts(key, realm);
        record = record.withNumTemporaryLockouts(updated);
    }

    @Override
    public void clearFailures() {
        ValkeyLoginFailureRecord cleared = provider.clearFailures(key, realm);
        if (cleared != null) {
            record = cleared;
        }
    }

    @Override
    public long getLastFailure() {
        return record.getLastFailure();
    }

    @Override
    public void setLastFailure(long lastFailure) {
        long updated = provider.updateLastFailure(key, realm, lastFailure);
        record = record.withLastFailure(updated);
    }

    @Override
    public String getLastIPFailure() {
        return record.getLastIpFailure();
    }

    @Override
    public void setLastIPFailure(String ip) {
        provider.updateLastIpFailure(key, realm, ip);
        record = record.withLastIpFailure(ip);
    }

    @Override
    public String getId() {
        return key;
    }
}
