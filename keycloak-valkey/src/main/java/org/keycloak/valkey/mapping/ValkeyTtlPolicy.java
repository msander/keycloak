package org.keycloak.valkey.mapping;

/**
 * Describes how time-to-live semantics are applied to data stored within Valkey.
 */
public enum ValkeyTtlPolicy {
    /**
     * Entries do not expire automatically; invalidation is coordinated through revision caches or explicit deletes.
     */
    NONE,
    /**
     * Entries carry an absolute expiration timestamp derived from domain rules such as session or token lifespan.
     */
    ABSOLUTE_EXPIRATION,
    /**
     * Entries rely on external mechanisms to manage lifecycle (for example, stream trimming driven by consumers).
     */
    CLIENT_MANAGED
}
