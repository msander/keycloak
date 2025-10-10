package org.keycloak.valkey.keys;

import org.keycloak.cluster.ClusterEvent;

/**
 * Broadcast event requesting all nodes to clear the public key cache.
 */
public final class ValkeyClearCacheEvent implements ClusterEvent {

    private static final ValkeyClearCacheEvent INSTANCE = new ValkeyClearCacheEvent();

    private ValkeyClearCacheEvent() {
    }

    public static ValkeyClearCacheEvent getInstance() {
        return INSTANCE;
    }
}
