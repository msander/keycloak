package org.keycloak.valkey.cluster;

import java.util.Objects;

import org.keycloak.cluster.ClusterEvent;

/**
 * Simple cluster event used for tests to avoid coupling on Infinispan-specific event definitions.
 */
final class TestClusterEvent implements ClusterEvent {

    private final String id;
    private final String message;

    TestClusterEvent(String id, String message) {
        this.id = id;
        this.message = message;
    }

    String id() {
        return id;
    }

    String message() {
        return message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TestClusterEvent that)) return false;
        return Objects.equals(id, that.id) && Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, message);
    }

    @Override
    public String toString() {
        return "TestClusterEvent{" +
                "id='" + id + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}
