package org.keycloak.valkey.mapping;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Catalogue describing how existing Keycloak caches map to Valkey data structures.
 */
public final class ValkeySpiMappings {

    public static final String REALM_CACHE = "realms";
    public static final String REALM_REVISIONS_CACHE = "realmRevisions";
    public static final String USER_CACHE = "users";
    public static final String USER_REVISIONS_CACHE = "userRevisions";
    public static final String AUTHORIZATION_CACHE = "authorization";
    public static final String AUTHORIZATION_REVISIONS_CACHE = "authorizationRevisions";
    public static final String KEYS_CACHE = "keys";
    public static final String CRL_CACHE = "crl";
    public static final String USER_SESSION_CACHE = "sessions";
    public static final String CLIENT_SESSION_CACHE = "clientSessions";
    public static final String OFFLINE_USER_SESSION_CACHE = "offlineSessions";
    public static final String OFFLINE_CLIENT_SESSION_CACHE = "offlineClientSessions";
    public static final String LOGIN_FAILURE_CACHE = "loginFailures";
    public static final String AUTHENTICATION_SESSIONS_CACHE = "authenticationSessions";
    public static final String ACTION_TOKEN_CACHE = "actionTokens";
    public static final String WORK_CACHE = "work";

    private static final List<ValkeyCacheDescriptor> DESCRIPTORS = List.of(
            new ValkeyCacheDescriptor(
                    REALM_CACHE,
                    "RealmCacheProvider",
                    ValkeyCacheStructure.HASH,
                    "realm:{realmId}",
                    ValkeyTtlPolicy.NONE,
                    false,
                    "Realm metadata cached locally and invalidated via revision counters."),
            new ValkeyCacheDescriptor(
                    REALM_REVISIONS_CACHE,
                    "RealmCacheProvider",
                    ValkeyCacheStructure.STRING,
                    "realm-rev:{realmId}",
                    ValkeyTtlPolicy.NONE,
                    false,
                    "Revision number tracking for realm cache invalidation."),
            new ValkeyCacheDescriptor(
                    USER_CACHE,
                    "UserCacheProvider",
                    ValkeyCacheStructure.HASH,
                    "user:{realmId}:{userId}",
                    ValkeyTtlPolicy.NONE,
                    false,
                    "Cached user representations scoped per realm with revision-driven invalidation."),
            new ValkeyCacheDescriptor(
                    USER_REVISIONS_CACHE,
                    "UserCacheProvider",
                    ValkeyCacheStructure.STRING,
                    "user-rev:{realmId}:{userId}",
                    ValkeyTtlPolicy.NONE,
                    false,
                    "User cache revision counter aiding cluster-wide invalidations."),
            new ValkeyCacheDescriptor(
                    AUTHORIZATION_CACHE,
                    "AuthorizationProvider",
                    ValkeyCacheStructure.HASH,
                    "authz:{realmId}:{resourceId}",
                    ValkeyTtlPolicy.NONE,
                    false,
                    "Authorization decisions cached locally alongside revision metadata."),
            new ValkeyCacheDescriptor(
                    AUTHORIZATION_REVISIONS_CACHE,
                    "AuthorizationProvider",
                    ValkeyCacheStructure.STRING,
                    "authz-rev:{realmId}:{resourceId}",
                    ValkeyTtlPolicy.NONE,
                    false,
                    "Authorization cache revision records."),
            new ValkeyCacheDescriptor(
                    KEYS_CACHE,
                    "PublicKeyStorageProvider",
                    ValkeyCacheStructure.HASH,
                    "keys:{realmId}:{keyId}",
                    ValkeyTtlPolicy.ABSOLUTE_EXPIRATION,
                    false,
                    "Realm key material cached with deterministic expiry respecting rotation policies."),
            new ValkeyCacheDescriptor(
                    CRL_CACHE,
                    "TruststoreProvider",
                    ValkeyCacheStructure.HASH,
                    "crl:{realmId}:{keyId}",
                    ValkeyTtlPolicy.ABSOLUTE_EXPIRATION,
                    false,
                    "Certificate revocation lists cached per realm entry with explicit expiry."),
            new ValkeyCacheDescriptor(
                    USER_SESSION_CACHE,
                    "UserSessionProvider",
                    ValkeyCacheStructure.HASH,
                    "user-session:{realmId}:{sessionId}",
                    ValkeyTtlPolicy.ABSOLUTE_EXPIRATION,
                    true,
                    "Online user sessions replicated cluster-wide with TTL tied to session lifespan."),
            new ValkeyCacheDescriptor(
                    CLIENT_SESSION_CACHE,
                    "UserSessionProvider",
                    ValkeyCacheStructure.HASH,
                    "client-session:{realmId}:{sessionId}",
                    ValkeyTtlPolicy.ABSOLUTE_EXPIRATION,
                    true,
                    "Client session state co-located with user sessions and expiring in lock-step."),
            new ValkeyCacheDescriptor(
                    OFFLINE_USER_SESSION_CACHE,
                    "UserSessionProvider",
                    ValkeyCacheStructure.HASH,
                    "offline-user-session:{realmId}:{sessionId}",
                    ValkeyTtlPolicy.ABSOLUTE_EXPIRATION,
                    true,
                    "Offline user sessions persisted with TTL derived from offline policies."),
            new ValkeyCacheDescriptor(
                    OFFLINE_CLIENT_SESSION_CACHE,
                    "UserSessionProvider",
                    ValkeyCacheStructure.HASH,
                    "offline-client-session:{realmId}:{sessionId}",
                    ValkeyTtlPolicy.ABSOLUTE_EXPIRATION,
                    true,
                    "Offline client sessions sharing lifecycle semantics with their user session."),
            new ValkeyCacheDescriptor(
                    LOGIN_FAILURE_CACHE,
                    "UserLoginFailureProvider",
                    ValkeyCacheStructure.HASH,
                    "login-failure:{realmId}:{userId}",
                    ValkeyTtlPolicy.ABSOLUTE_EXPIRATION,
                    true,
                    "Failed login counters expiring per realm lockout settings."),
            new ValkeyCacheDescriptor(
                    AUTHENTICATION_SESSIONS_CACHE,
                    "AuthenticationSessionProvider",
                    ValkeyCacheStructure.HASH,
                    "auth-session:{realmId}:{rootSessionId}",
                    ValkeyTtlPolicy.ABSOLUTE_EXPIRATION,
                    true,
                    "Root authentication sessions with TTL bound to authentication lifespan."),
            new ValkeyCacheDescriptor(
                    ACTION_TOKEN_CACHE,
                    "SingleUseObjectProvider",
                    ValkeyCacheStructure.HASH,
                    "action-token:{tokenId}",
                    ValkeyTtlPolicy.ABSOLUTE_EXPIRATION,
                    true,
                    "Single-use token payloads supported by a sorted-set index for sweeps."),
            new ValkeyCacheDescriptor(
                    WORK_CACHE,
                    "ClusterProvider (WorkCache)",
                    ValkeyCacheStructure.STREAM,
                    "work:{realmId}",
                    ValkeyTtlPolicy.CLIENT_MANAGED,
                    true,
                    "Cluster task coordination backed by Redis Streams and consumer groups."));

    private static final Map<String, ValkeyCacheDescriptor> BY_CACHE = indexByCacheName(DESCRIPTORS);

    private ValkeySpiMappings() {
    }

    private static Map<String, ValkeyCacheDescriptor> indexByCacheName(List<ValkeyCacheDescriptor> descriptors) {
        Map<String, ValkeyCacheDescriptor> byName = descriptors.stream()
                .collect(Collectors.toMap(ValkeyCacheDescriptor::cacheName, Function.identity(),
                        (left, right) -> {
                            throw new IllegalStateException(
                                    "Duplicate Valkey cache descriptor defined for cache '%s'".formatted(left.cacheName()));
                        },
                        LinkedHashMap::new));
        return Map.copyOf(byName);
    }

    /**
     * Provides the immutable catalogue of cache descriptors.
     */
    public static List<ValkeyCacheDescriptor> descriptors() {
        return DESCRIPTORS;
    }

    /**
     * Looks up a descriptor by the canonical cache name.
     *
     * @param cacheName the Keycloak cache name as used by existing SPIs
     * @return matching descriptor when present
     */
    public static Optional<ValkeyCacheDescriptor> byCacheName(String cacheName) {
        if (cacheName == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(BY_CACHE.get(cacheName));
    }
}
