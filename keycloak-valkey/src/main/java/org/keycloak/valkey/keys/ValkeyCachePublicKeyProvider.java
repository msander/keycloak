package org.keycloak.valkey.keys;

import java.util.concurrent.ConcurrentMap;

import org.keycloak.models.KeycloakSession;
import org.keycloak.models.cache.CachePublicKeyProvider;
import org.keycloak.valkey.cluster.ValkeyClusterProviderResolver;

/**
 * Cache facade clearing entries from the shared public key cache and propagating cluster events.
 */
final class ValkeyCachePublicKeyProvider implements CachePublicKeyProvider {

    private final KeycloakSession session;
    private final ConcurrentMap<String, ValkeyPublicKeysEntry> cache;

    ValkeyCachePublicKeyProvider(KeycloakSession session, ConcurrentMap<String, ValkeyPublicKeysEntry> cache) {
        this.session = session;
        this.cache = cache;
    }

    @Override
    public void clearCache() {
        cache.clear();
        var cluster = ValkeyClusterProviderResolver.resolve(session, null);
        if (cluster != null) {
            cluster.notify(ValkeyPublicKeyStorageProviderFactory.KEYS_CLEAR_CACHE_EVENT,
                    ValkeyClearCacheEvent.getInstance(), true);
        }
    }

    @Override
    public void close() {
    }
}
