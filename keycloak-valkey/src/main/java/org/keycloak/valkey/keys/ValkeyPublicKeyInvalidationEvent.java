package org.keycloak.valkey.keys;

import org.keycloak.cluster.ClusterEvent;

/**
 * Cluster event requesting eviction of a cached public key entry.
 */
public final class ValkeyPublicKeyInvalidationEvent implements ClusterEvent {

    private final String cacheKey;

    private ValkeyPublicKeyInvalidationEvent(String cacheKey) {
        this.cacheKey = cacheKey;
    }

    public static ValkeyPublicKeyInvalidationEvent create(String cacheKey) {
        return new ValkeyPublicKeyInvalidationEvent(cacheKey);
    }

    public String getCacheKey() {
        return cacheKey;
    }
}
