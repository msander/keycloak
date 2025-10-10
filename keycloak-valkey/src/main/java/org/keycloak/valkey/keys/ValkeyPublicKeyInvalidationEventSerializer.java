package org.keycloak.valkey.keys;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.keycloak.valkey.cluster.ValkeyClusterEventSerializer;

/**
 * Serializer for {@link ValkeyPublicKeyInvalidationEvent} leveraging UTF-8 encoded cache keys.
 */
public final class ValkeyPublicKeyInvalidationEventSerializer
        implements ValkeyClusterEventSerializer<ValkeyPublicKeyInvalidationEvent> {

    private static final String TYPE_ID = "keycloak.valkey.keys.invalidation";

    @Override
    public String getTypeId() {
        return TYPE_ID;
    }

    @Override
    public Class<ValkeyPublicKeyInvalidationEvent> getEventType() {
        return ValkeyPublicKeyInvalidationEvent.class;
    }

    @Override
    public byte[] serialize(ValkeyPublicKeyInvalidationEvent event) {
        return event.getCacheKey().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public ValkeyPublicKeyInvalidationEvent deserialize(byte[] payload) throws IOException {
        return ValkeyPublicKeyInvalidationEvent.create(new String(payload, StandardCharsets.UTF_8));
    }
}
