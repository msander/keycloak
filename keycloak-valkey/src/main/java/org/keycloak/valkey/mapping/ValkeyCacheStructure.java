package org.keycloak.valkey.mapping;

/**
 * Enumerates the Redis/Valkey data structures that back individual Keycloak caches.
 */
public enum ValkeyCacheStructure {
    HASH,
    STRING,
    LIST,
    SORTED_SET,
    STREAM
}
