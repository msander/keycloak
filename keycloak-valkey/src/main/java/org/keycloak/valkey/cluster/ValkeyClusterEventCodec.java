package org.keycloak.valkey.cluster;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;

import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.config.Configuration;
import org.jboss.logging.Logger;
import org.keycloak.cluster.ClusterEvent;
import org.keycloak.cluster.ClusterProvider;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;

/**
 * Encodes and decodes {@link ClusterEvent} collections for transport over Redis Pub/Sub using Protostream schemas
 * already present on the Keycloak classpath.
 */
final class ValkeyClusterEventCodec {

    private static final Logger logger = Logger.getLogger(ValkeyClusterEventCodec.class);

    private final SerializationContext context;

    ValkeyClusterEventCodec() {
        this.context = createContext();
    }

    byte[] encode(String eventKey, Collection<? extends ClusterEvent> events, boolean ignoreSender,
            ClusterProvider.DCNotify dcNotify, String senderNodeId, String senderSite) {
        Objects.requireNonNull(eventKey, "eventKey");
        Objects.requireNonNull(events, "events");
        Objects.requireNonNull(dcNotify, "dcNotify");
        SiteFilter filter = SiteFilter.from(dcNotify);
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            CodedOutputStream writer = CodedOutputStream.newInstance(baos);
            writer.writeString(1, eventKey);
            if (senderNodeId != null) {
                writer.writeString(2, senderNodeId);
            }
            if (senderSite != null) {
                writer.writeString(3, senderSite);
            }
            writer.writeEnum(4, filter.ordinal());
            writer.writeBool(5, ignoreSender);
            for (ClusterEvent event : events) {
                byte[] payload = ProtobufUtil.toWrappedByteArray(context, event);
                writer.writeByteArray(6, payload);
            }
            writer.flush();
            return baos.toByteArray();
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to encode cluster event payload", ex);
        }
    }

    DecodedMessage decode(byte[] data) {
        Objects.requireNonNull(data, "data");
        try {
            CodedInputStream reader = CodedInputStream.newInstance(data);
            String eventKey = null;
            String senderNode = null;
            String senderSite = null;
            SiteFilter filter = SiteFilter.ALL;
            boolean ignoreSender = false;
            List<ClusterEvent> events = new ArrayList<>();
            int tag;
            while ((tag = reader.readTag()) != 0) {
                switch (WireFormat.getTagFieldNumber(tag)) {
                    case 1 -> eventKey = reader.readStringRequireUtf8();
                    case 2 -> senderNode = reader.readStringRequireUtf8();
                    case 3 -> senderSite = reader.readStringRequireUtf8();
                    case 4 -> filter = resolveFilter(reader.readEnum());
                    case 5 -> ignoreSender = reader.readBool();
                    case 6 -> {
                        byte[] payload = reader.readByteArray();
                        ClusterEvent event = decodeEvent(payload);
                        events.add(event);
                    }
                    default -> reader.skipField(tag);
                }
            }
            if (eventKey == null) {
                throw new IllegalStateException("Cluster event payload missing event key");
            }
            return new DecodedMessage(eventKey, events, senderNode, senderSite, filter, ignoreSender);
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

    private ClusterEvent decodeEvent(byte[] payload) throws IOException {
        return (ClusterEvent) ProtobufUtil.fromWrappedByteArray(context, payload);
    }

    private SerializationContext createContext() {
        SerializationContext ctx = ProtobufUtil.newSerializationContext(Configuration.builder().build());
        ServiceLoader<SerializationContextInitializer> loader = ServiceLoader.load(SerializationContextInitializer.class);
        for (SerializationContextInitializer initializer : loader) {
            try {
                initializer.registerSchema(ctx);
                initializer.registerMarshallers(ctx);
            } catch (RuntimeException ex) {
                logger.debugf(ex, "Failed to register protostream schema from %s", initializer.getClass());
            }
        }
        return ctx;
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
