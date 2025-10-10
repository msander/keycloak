package org.keycloak.valkey.keys;

import org.keycloak.crypto.PublicKeysWrapper;

/**
 * Container holding cached public keys and the last time they were requested.
 */
final class ValkeyPublicKeysEntry {

    private final int lastRequestTime;
    private final PublicKeysWrapper keys;

    ValkeyPublicKeysEntry(int lastRequestTime, PublicKeysWrapper keys) {
        this.lastRequestTime = lastRequestTime;
        this.keys = keys;
    }

    int getLastRequestTime() {
        return lastRequestTime;
    }

    PublicKeysWrapper getKeys() {
        return keys;
    }
}
