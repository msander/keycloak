package org.keycloak.valkey.cluster;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

import org.jboss.logging.Logger;
import org.keycloak.cluster.ClusterEvent;
import org.keycloak.cluster.ClusterProvider;

/**
 * Encodes and decodes {@link ClusterEvent} collections for transport over Redis Pub/Sub using pluggable
 * {@link ValkeyClusterEventSerializer} implementations discovered via the {@link ServiceLoader}.
 */
final class ValkeyClusterEventCodec {

    private static final Logger logger = Logger.getLogger(ValkeyClusterEventCodec.class);

    private final SerializerRegistry serializers;

    ValkeyClusterEventCodec() {
        this.serializers = SerializerRegistry.load();
    }

    byte[] encode(String eventKey, Collection<? extends ClusterEvent> events, boolean ignoreSender,
            ClusterProvider.DCNotify dcNotify, String senderNodeId, String senderSite) {
        Objects.requireNonNull(eventKey, "eventKey");
        Objects.requireNonNull(events, "events");
        Objects.requireNonNull(dcNotify, "dcNotify");
        SiteFilter filter = SiteFilter.from(dcNotify);
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(baos)) {
                writeString(out, eventKey);
                writeNullableString(out, senderNodeId);
                writeNullableString(out, senderSite);
                out.writeByte(filter.ordinal());
                out.writeBoolean(ignoreSender);
                out.writeInt(events.size());
                for (ClusterEvent event : events) {
                    serializers.encode(event, out);
                }
                out.flush();
                return baos.toByteArray();
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to encode cluster event payload", ex);
        }
    }

    DecodedMessage decode(byte[] data) {
        Objects.requireNonNull(data, "data");
        try {
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(data))) {
                String eventKey = readString(in);
                String senderNode = readNullableString(in);
                String senderSite = readNullableString(in);
                SiteFilter filter = resolveFilter(Byte.toUnsignedInt(in.readByte()));
                boolean ignoreSender = in.readBoolean();
                int eventCount = in.readInt();
                if (eventCount < 0) {
                    throw new IllegalStateException("Negative event count in cluster payload");
                }
                List<ClusterEvent> events = new ArrayList<>(eventCount);
                for (int i = 0; i < eventCount; i++) {
                    events.add(serializers.decode(in));
                }
                return new DecodedMessage(eventKey, events, senderNode, senderSite, filter, ignoreSender);
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to decode cluster event payload", ex);
        }
    }

    private SiteFilter resolveFilter(int ordinal) {
        SiteFilter[] values = SiteFilter.values();
        if (ordinal < 0 || ordinal >= values.length) {
            logger.debugf("Unknown site filter ordinal %d; defaulting to ALL", ordinal);
            return SiteFilter.ALL;
        }
        return values[ordinal];
    }

    private static void writeString(DataOutput out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInput in) throws IOException {
        int length = in.readInt();
        if (length < 0) {
            throw new IllegalStateException("Negative string length in cluster payload");
        }
        byte[] data = new byte[length];
        in.readFully(data);
        return new String(data, StandardCharsets.UTF_8);
    }

    private static void writeNullableString(DataOutput out, String value) throws IOException {
        if (value == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            writeString(out, value);
        }
    }

    private static String readNullableString(DataInput in) throws IOException {
        boolean present = in.readBoolean();
        return present ? readString(in) : null;
    }

    private static final class SerializerRegistry {

        private final Map<String, ValkeyClusterEventSerializer<?>> byTypeId;
        private final List<ValkeyClusterEventSerializer<?>> serializers;

        private SerializerRegistry(Map<String, ValkeyClusterEventSerializer<?>> byTypeId,
                List<ValkeyClusterEventSerializer<?>> serializers) {
            this.byTypeId = Map.copyOf(byTypeId);
            this.serializers = List.copyOf(serializers);
        }

        static SerializerRegistry load() {
            Map<String, ValkeyClusterEventSerializer<?>> byId = new HashMap<>();
            List<ValkeyClusterEventSerializer<?>> serializers = new ArrayList<>();
            @SuppressWarnings("rawtypes")
            ServiceLoader<ValkeyClusterEventSerializer> loader = ServiceLoader.load(ValkeyClusterEventSerializer.class);
            for (ValkeyClusterEventSerializer serializer : loader) {
                ValkeyClusterEventSerializer<?> typed = serializer;
                ValkeyClusterEventSerializer<?> existing = byId.putIfAbsent(typed.getTypeId(), typed);
                if (existing != null) {
                    throw new IllegalStateException("Duplicate Valkey cluster event serializer for type "
                            + typed.getTypeId() + " from " + typed.getClass() + " and " + existing.getClass());
                }
                serializers.add(typed);
            }
            return new SerializerRegistry(byId, serializers);
        }

        void encode(ClusterEvent event, DataOutput out) throws IOException {
            ValkeyClusterEventSerializer<ClusterEvent> serializer = findSerializer(event.getClass());
            byte[] payload = serializer.serialize(event);
            writeString(out, serializer.getTypeId());
            out.writeInt(payload.length);
            out.write(payload);
        }

        ClusterEvent decode(DataInput in) throws IOException {
            String typeId = readString(in);
            ValkeyClusterEventSerializer<?> serializer = byTypeId.get(typeId);
            if (serializer == null) {
                throw new IllegalStateException("No Valkey cluster event serializer registered for type " + typeId);
            }
            int length = in.readInt();
            if (length < 0) {
                throw new IllegalStateException("Negative payload length for cluster event type " + typeId);
            }
            byte[] payload = new byte[length];
            in.readFully(payload);
            return deserialize(serializer, payload);
        }

        @SuppressWarnings("unchecked")
        private ValkeyClusterEventSerializer<ClusterEvent> findSerializer(Class<? extends ClusterEvent> eventType) {
            ValkeyClusterEventSerializer<?> bestMatch = null;
            int bestDepth = Integer.MAX_VALUE;
            for (ValkeyClusterEventSerializer<?> serializer : serializers) {
                Class<?> supported = serializer.getEventType();
                if (!supported.isAssignableFrom(eventType)) {
                    continue;
                }
                int depth = hierarchyDistance(eventType, supported);
                if (depth < bestDepth) {
                    bestDepth = depth;
                    bestMatch = serializer;
                }
            }
            if (bestMatch == null) {
                throw new IllegalStateException("No Valkey cluster event serializer registered for event class "
                        + eventType.getName());
            }
            return (ValkeyClusterEventSerializer<ClusterEvent>) bestMatch;
        }

        private int hierarchyDistance(Class<?> type, Class<?> target) {
            int depth = 0;
            Class<?> current = type;
            while (current != null) {
                if (target.isAssignableFrom(current)) {
                    return depth;
                }
                current = current.getSuperclass();
                depth++;
            }
            return Integer.MAX_VALUE / 2;
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        private ClusterEvent deserialize(ValkeyClusterEventSerializer serializer, byte[] payload) throws IOException {
            return (ClusterEvent) serializer.deserialize(payload);
        }
    }

    enum SiteFilter {
        ALL,
        LOCAL,
        REMOTE;

        static SiteFilter from(ClusterProvider.DCNotify notify) {
            return switch (notify) {
                case LOCAL_DC_ONLY -> LOCAL;
                case ALL_BUT_LOCAL_DC -> REMOTE;
                case ALL_DCS -> ALL;
            };
        }

        boolean allows(String senderSite, String localSite) {
            return switch (this) {
                case ALL -> true;
                case LOCAL -> Objects.equals(senderSite, localSite);
                case REMOTE -> !Objects.equals(senderSite, localSite);
            };
        }
    }

    static final class DecodedMessage {
        private final String eventKey;
        private final Collection<ClusterEvent> events;
        private final String senderNodeId;
        private final String senderSite;
        private final SiteFilter siteFilter;
        private final boolean ignoreSender;

        DecodedMessage(String eventKey, Collection<ClusterEvent> events, String senderNodeId, String senderSite,
                SiteFilter siteFilter, boolean ignoreSender) {
            this.eventKey = eventKey;
            this.events = List.copyOf(events);
            this.senderNodeId = senderNodeId;
            this.senderSite = senderSite;
            this.siteFilter = siteFilter;
            this.ignoreSender = ignoreSender;
        }

        String eventKey() {
            return eventKey;
        }

        Collection<ClusterEvent> events() {
            return events;
        }

        String senderNodeId() {
            return senderNodeId;
        }

        String senderSite() {
            return senderSite;
        }

        boolean ignoreSender() {
            return ignoreSender;
        }

        boolean shouldDeliver(String localNodeId, String localSite) {
            if (senderNodeId != null && senderNodeId.equals(localNodeId)) {
                return false;
            }
            return siteFilter.allows(senderSite, localSite);
        }
    }
}
