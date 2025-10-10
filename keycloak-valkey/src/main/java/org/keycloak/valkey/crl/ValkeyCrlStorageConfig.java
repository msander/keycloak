package org.keycloak.valkey.crl;

import org.keycloak.Config;

final class ValkeyCrlStorageConfig {

    static final String DEFAULT_NAMESPACE = "keycloak:crl";

    private final String namespace;
    private final long cacheTimeMillis;
    private final long minTimeBetweenRequestsMillis;

    private ValkeyCrlStorageConfig(String namespace, long cacheTimeMillis, long minTimeBetweenRequestsMillis) {
        this.namespace = namespace;
        this.cacheTimeMillis = cacheTimeMillis;
        this.minTimeBetweenRequestsMillis = minTimeBetweenRequestsMillis;
    }

    static ValkeyCrlStorageConfig from(Config.Scope scope) {
        if (scope == null) {
            return new ValkeyCrlStorageConfig(DEFAULT_NAMESPACE, -1L, 10_000L);
        }
        String namespace = scope.get("namespace", DEFAULT_NAMESPACE);
        long cacheTimeSeconds = scope.getLong("cacheTime", -1L);
        long minBetweenSeconds = scope.getLong("minTimeBetweenRequests", 10L);
        long cacheMillis = cacheTimeSeconds > 0 ? cacheTimeSeconds * 1000L : -1L;
        long minMillis = minBetweenSeconds > 0 ? minBetweenSeconds * 1000L : 10_000L;
        return new ValkeyCrlStorageConfig(namespace, cacheMillis, minMillis);
    }

    String key(String crlKey) {
        return namespace + ':' + crlKey;
    }

    String namespace() {
        return namespace;
    }

    long cacheTimeMillis() {
        return cacheTimeMillis;
    }

    long minTimeBetweenRequestsMillis() {
        return minTimeBetweenRequestsMillis;
    }
}
