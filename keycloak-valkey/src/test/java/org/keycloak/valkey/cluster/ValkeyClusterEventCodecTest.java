package org.keycloak.valkey.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.keycloak.cluster.ClusterEvent;
import org.keycloak.cluster.ClusterProvider;

class ValkeyClusterEventCodecTest {

    private final ValkeyClusterEventCodec codec = new ValkeyClusterEventCodec();

    @Test
    void shouldEncodeAndDecodeEvents() {
        ClusterEvent event = new TestClusterEvent("1", "clear");

        byte[] payload = codec.encode("channel", List.of(event), true, ClusterProvider.DCNotify.ALL_DCS, "node-1", "site-a");
        ValkeyClusterEventCodec.DecodedMessage decoded = codec.decode(payload);

        assertEquals("channel", decoded.eventKey());
        assertEquals("node-1", decoded.senderNodeId());
        assertEquals("site-a", decoded.senderSite());
        assertTrue(decoded.ignoreSender());
        assertEquals(1, decoded.events().size());
        assertEquals(event.getClass(), decoded.events().iterator().next().getClass());
        assertTrue(decoded.shouldDeliver("other", "site-a"));
    }

    @Test
    void shouldRespectIgnoreSenderAndSiteFilter() {
        ClusterEvent event = new TestClusterEvent("1", "clear");
        byte[] payload = codec.encode("channel", List.of(event), true, ClusterProvider.DCNotify.LOCAL_DC_ONLY, "node-1", "site-a");
        ValkeyClusterEventCodec.DecodedMessage decoded = codec.decode(payload);

        assertFalse(decoded.shouldDeliver("node-1", "site-a"));
        assertTrue(decoded.shouldDeliver("node-2", "site-a"));
        assertFalse(decoded.shouldDeliver("node-2", "site-b"));
    }

    @Test
    void shouldDefaultUnknownSiteFilterToAll() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos)) {
            writeString(out, "channel");
            out.writeBoolean(true);
            writeString(out, "node-1");
            out.writeBoolean(true);
            writeString(out, "site-a");
            out.writeByte(42);
            out.writeBoolean(false);
            out.writeInt(0);
        }

        ValkeyClusterEventCodec.DecodedMessage decoded = codec.decode(baos.toByteArray());
        assertTrue(decoded.shouldDeliver("node-2", "any"));
    }

    @Test
    void shouldRejectMissingEventKey() {
        assertThrows(IllegalStateException.class, () -> codec.decode(new byte[0]));
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }
}
