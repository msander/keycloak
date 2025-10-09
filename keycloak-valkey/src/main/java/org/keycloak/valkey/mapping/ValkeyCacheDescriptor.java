package org.keycloak.valkey.mapping;

import java.util.Objects;

/**
 * Immutable description of how a Keycloak cache or SPI maps onto Valkey data structures.
 */
public record ValkeyCacheDescriptor(
        String cacheName,
        String primarySpi,
        ValkeyCacheStructure structure,
        String keyPattern,
        ValkeyTtlPolicy ttlPolicy,
        boolean clustered,
        String notes) {

    public ValkeyCacheDescriptor {
        cacheName = requireText(cacheName, "cacheName");
        primarySpi = requireText(primarySpi, "primarySpi");
        structure = Objects.requireNonNull(structure, "structure");
        keyPattern = requireText(keyPattern, "keyPattern");
        ttlPolicy = Objects.requireNonNull(ttlPolicy, "ttlPolicy");
        notes = requireText(notes, "notes");
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return trimmed;
    }
}
