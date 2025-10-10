package org.keycloak.valkey.cluster;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Serializer used in tests to round-trip {@link TestClusterEvent} instances without Protostream.
 */
public final class TestClusterEventSerializer implements ValkeyClusterEventSerializer<TestClusterEvent> {

    private static final String TYPE_ID = "keycloak.valkey.test.event";

    @Override
    public String getTypeId() {
        return TYPE_ID;
    }

    @Override
    public Class<TestClusterEvent> getEventType() {
        return TestClusterEvent.class;
    }

    @Override
    public byte[] serialize(TestClusterEvent event) {
        String payload = event.id() + '\n' + event.message();
        return payload.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public TestClusterEvent deserialize(byte[] payload) throws IOException {
        String[] parts = new String(payload, StandardCharsets.UTF_8).split("\\n", 2);
        if (parts.length != 2) {
            throw new IOException("Malformed test event payload");
        }
        return new TestClusterEvent(parts[0], parts[1]);
    }
}
