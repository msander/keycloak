package org.keycloak.valkey.keys;

import java.io.IOException;

import org.keycloak.valkey.cluster.ValkeyClusterEventSerializer;

/**
 * Serializer for {@link ValkeyClearCacheEvent} which carries no state.
 */
public final class ValkeyClearCacheEventSerializer implements ValkeyClusterEventSerializer<ValkeyClearCacheEvent> {

    private static final String TYPE_ID = "keycloak.valkey.keys.clear";

    @Override
    public String getTypeId() {
        return TYPE_ID;
    }

    @Override
    public Class<ValkeyClearCacheEvent> getEventType() {
        return ValkeyClearCacheEvent.class;
    }

    @Override
    public byte[] serialize(ValkeyClearCacheEvent event) {
        return new byte[0];
    }

    @Override
    public ValkeyClearCacheEvent deserialize(byte[] payload) throws IOException {
        return ValkeyClearCacheEvent.getInstance();
    }
}
