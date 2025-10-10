package org.keycloak.valkey.cluster;

import java.io.IOException;

import org.keycloak.cluster.ClusterEvent;

/**
 * Service-provider interface for serializing {@link ClusterEvent} instances so they can be transported over
 * Valkey's pub/sub channels without depending on Infinispan Protostream codecs.
 *
 * @param <T> concrete event type supported by the serializer
 */
public interface ValkeyClusterEventSerializer<T extends ClusterEvent> {

    /**
     * @return identifier written to the wire so the matching serializer can be located during decoding. Must be unique
     *         within the application.
     */
    String getTypeId();

    /**
     * @return event type this serializer supports. The serializer will be used for subclasses of this type as well.
     */
    Class<T> getEventType();

    /**
     * Serializes the supplied event into an opaque binary payload.
     *
     * @param event concrete event to encode
     * @return encoded payload bytes (may be empty)
     * @throws IOException when encoding fails
     */
    byte[] serialize(T event) throws IOException;

    /**
     * Reconstructs an event instance from the binary payload previously produced by {@link #serialize(ClusterEvent)}.
     *
     * @param payload encoded event payload
     * @return decoded event instance
     * @throws IOException when decoding fails
     */
    T deserialize(byte[] payload) throws IOException;
}

