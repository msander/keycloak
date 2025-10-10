package org.keycloak.valkey.cluster;

import java.util.Objects;

import org.keycloak.cluster.ClusterEvent;

/**
 * Internal event signalling that a clustered work item finished execution.
 */
final class ValkeyWorkCompletionEvent implements ClusterEvent {

    private final String lockKey;
    private final boolean success;

    ValkeyWorkCompletionEvent(String lockKey, boolean success) {
        this.lockKey = Objects.requireNonNull(lockKey, "lockKey");
        this.success = success;
    }

    String lockKey() {
        return lockKey;
    }

    boolean success() {
        return success;
    }
}

