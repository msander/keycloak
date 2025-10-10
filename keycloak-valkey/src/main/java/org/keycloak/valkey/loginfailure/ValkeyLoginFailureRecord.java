package org.keycloak.valkey.loginfailure;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable snapshot of a user login failure entry stored in Valkey.
 */
final class ValkeyLoginFailureRecord {

    static final String FIELD_REALM_ID = "realmId";
    static final String FIELD_USER_ID = "userId";
    static final String FIELD_FAILED_LOGIN_NOT_BEFORE = "failedLoginNotBefore";
    static final String FIELD_NUM_FAILURES = "numFailures";
    static final String FIELD_NUM_TEMPORARY_LOCKOUTS = "numTemporaryLockouts";
    static final String FIELD_LAST_FAILURE = "lastFailure";
    static final String FIELD_LAST_IP_FAILURE = "lastIPFailure";

    private final String realmId;
    private final String userId;
    private final int failedLoginNotBefore;
    private final int numFailures;
    private final int numTemporaryLockouts;
    private final long lastFailure;
    private final String lastIpFailure;

    private ValkeyLoginFailureRecord(String realmId, String userId, int failedLoginNotBefore, int numFailures,
            int numTemporaryLockouts, long lastFailure, String lastIpFailure) {
        this.realmId = Objects.requireNonNull(realmId, "realmId");
        this.userId = Objects.requireNonNull(userId, "userId");
        this.failedLoginNotBefore = failedLoginNotBefore;
        this.numFailures = numFailures;
        this.numTemporaryLockouts = numTemporaryLockouts;
        this.lastFailure = lastFailure;
        this.lastIpFailure = lastIpFailure;
    }

    static ValkeyLoginFailureRecord empty(String realmId, String userId) {
        return new ValkeyLoginFailureRecord(realmId, userId, 0, 0, 0, 0L, null);
    }

    static ValkeyLoginFailureRecord from(Map<String, String> payload) {
        if (payload == null || payload.isEmpty()) {
            return null;
        }
        String realmId = payload.get(FIELD_REALM_ID);
        String userId = payload.get(FIELD_USER_ID);
        if (realmId == null || userId == null) {
            return null;
        }
        int failedLoginNotBefore = parseInt(payload.get(FIELD_FAILED_LOGIN_NOT_BEFORE));
        int numFailures = parseInt(payload.get(FIELD_NUM_FAILURES));
        int numTemporaryLockouts = parseInt(payload.get(FIELD_NUM_TEMPORARY_LOCKOUTS));
        long lastFailure = parseLong(payload.get(FIELD_LAST_FAILURE));
        String lastIpFailure = payload.get(FIELD_LAST_IP_FAILURE);
        return new ValkeyLoginFailureRecord(realmId, userId, failedLoginNotBefore, numFailures, numTemporaryLockouts,
                lastFailure, lastIpFailure);
    }

    Map<String, String> toMap() {
        Map<String, String> payload = new HashMap<>();
        payload.put(FIELD_REALM_ID, realmId);
        payload.put(FIELD_USER_ID, userId);
        payload.put(FIELD_FAILED_LOGIN_NOT_BEFORE, Integer.toString(failedLoginNotBefore));
        payload.put(FIELD_NUM_FAILURES, Integer.toString(numFailures));
        payload.put(FIELD_NUM_TEMPORARY_LOCKOUTS, Integer.toString(numTemporaryLockouts));
        payload.put(FIELD_LAST_FAILURE, Long.toString(lastFailure));
        if (lastIpFailure != null) {
            payload.put(FIELD_LAST_IP_FAILURE, lastIpFailure);
        }
        return payload;
    }

    String getRealmId() {
        return realmId;
    }

    String getUserId() {
        return userId;
    }

    int getFailedLoginNotBefore() {
        return failedLoginNotBefore;
    }

    int getNumFailures() {
        return numFailures;
    }

    int getNumTemporaryLockouts() {
        return numTemporaryLockouts;
    }

    long getLastFailure() {
        return lastFailure;
    }

    String getLastIpFailure() {
        return lastIpFailure;
    }

    ValkeyLoginFailureRecord withFailedLoginNotBefore(int value) {
        return new ValkeyLoginFailureRecord(realmId, userId, value, numFailures, numTemporaryLockouts, lastFailure,
                lastIpFailure);
    }

    ValkeyLoginFailureRecord withNumFailures(int value) {
        return new ValkeyLoginFailureRecord(realmId, userId, failedLoginNotBefore, value, numTemporaryLockouts, lastFailure,
                lastIpFailure);
    }

    ValkeyLoginFailureRecord withNumTemporaryLockouts(int value) {
        return new ValkeyLoginFailureRecord(realmId, userId, failedLoginNotBefore, numFailures, value, lastFailure,
                lastIpFailure);
    }

    ValkeyLoginFailureRecord withLastFailure(long value) {
        return new ValkeyLoginFailureRecord(realmId, userId, failedLoginNotBefore, numFailures, numTemporaryLockouts, value,
                lastIpFailure);
    }

    ValkeyLoginFailureRecord withLastIpFailure(String value) {
        return new ValkeyLoginFailureRecord(realmId, userId, failedLoginNotBefore, numFailures, numTemporaryLockouts,
                lastFailure, value);
    }

    ValkeyLoginFailureRecord cleared() {
        return new ValkeyLoginFailureRecord(realmId, userId, 0, 0, 0, 0L, null);
    }

    private static int parseInt(String candidate) {
        if (candidate == null || candidate.isBlank()) {
            return 0;
        }
        return Integer.parseInt(candidate);
    }

    private static long parseLong(String candidate) {
        if (candidate == null || candidate.isBlank()) {
            return 0L;
        }
        return Long.parseLong(candidate);
    }
}
