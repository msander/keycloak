package org.keycloak.valkey.cluster;

import java.io.IOException;

import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;

public final class TestClusterEventSchema implements SerializationContextInitializer {

    public TestClusterEventSchema() {
    }

    private static final String PROTO_FILE_NAME = "keycloak-valkey-test-cluster-event.proto";
    private static final String PROTO_DEFINITION = "syntax=\"proto3\";\n" +
            "package keycloak.valkey.test;\n" +
            "message TestClusterEvent {\n" +
            "  string id = 1;\n" +
            "  string message = 2;\n" +
            "}";

    @Override
    public String getProtoFileName() {
        return PROTO_FILE_NAME;
    }

    @Override
    public String getProtoFile() {
        return PROTO_DEFINITION;
    }

    @Override
    public void registerSchema(SerializationContext context) {
        context.registerProtoFiles(FileDescriptorSource.fromString(PROTO_FILE_NAME, PROTO_DEFINITION));
    }

    @Override
    public void registerMarshallers(SerializationContext context) {
        context.registerMarshaller(new MessageMarshaller<TestClusterEvent>() {
            @Override
            public TestClusterEvent readFrom(MessageMarshaller.ProtoStreamReader reader) throws IOException {
                String id = reader.readString("id");
                String message = reader.readString("message");
                return new TestClusterEvent(id, message);
            }

            @Override
            public void writeTo(MessageMarshaller.ProtoStreamWriter writer, TestClusterEvent event) throws IOException {
                writer.writeString("id", event.id());
                writer.writeString("message", event.message());
            }

            @Override
            public Class<? extends TestClusterEvent> getJavaClass() {
                return TestClusterEvent.class;
            }

            @Override
            public String getTypeName() {
                return "keycloak.valkey.test.TestClusterEvent";
            }
        });
    }
}
