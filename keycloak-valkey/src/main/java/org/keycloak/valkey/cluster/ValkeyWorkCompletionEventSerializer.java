package org.keycloak.valkey.cluster;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Serializer for {@link ValkeyWorkCompletionEvent} instances.
 */
public final class ValkeyWorkCompletionEventSerializer
        implements ValkeyClusterEventSerializer<ValkeyWorkCompletionEvent> {

    private static final String TYPE_ID = "keycloak.valkey.cluster.work-completion";

    @Override
    public String getTypeId() {
        return TYPE_ID;
    }

    @Override
    public Class<ValkeyWorkCompletionEvent> getEventType() {
        return ValkeyWorkCompletionEvent.class;
    }

    @Override
    public byte[] serialize(ValkeyWorkCompletionEvent event) {
        String payload = event.lockKey() + '|' + (event.success() ? '1' : '0');
        return payload.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public ValkeyWorkCompletionEvent deserialize(byte[] payload) throws IOException {
        String value = new String(payload, StandardCharsets.UTF_8);
        int separator = value.lastIndexOf('|');
        if (separator < 0 || separator == value.length() - 1) {
            throw new IOException("Invalid Valkey work completion payload");
        }
        String lockKey = value.substring(0, separator);
        boolean success = value.charAt(separator + 1) == '1';
        return new ValkeyWorkCompletionEvent(lockKey, success);
    }
}

