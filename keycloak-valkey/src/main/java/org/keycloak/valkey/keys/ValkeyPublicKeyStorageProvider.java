package org.keycloak.valkey.keys;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.jboss.logging.Logger;
import org.keycloak.cluster.ClusterProvider;
import org.keycloak.common.util.Time;
import org.keycloak.crypto.KeyWrapper;
import org.keycloak.crypto.PublicKeysWrapper;
import org.keycloak.keys.PublicKeyLoader;
import org.keycloak.keys.PublicKeyStorageProvider;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakTransaction;
import org.keycloak.models.KeycloakTransactionManager;

/**
 * Public key storage provider backed by an in-memory cache with Valkey-based invalidations.
 */
final class ValkeyPublicKeyStorageProvider implements PublicKeyStorageProvider {

    private static final Logger logger = Logger.getLogger(ValkeyPublicKeyStorageProvider.class);

    private final KeycloakSession session;
    private final ConcurrentMap<String, ValkeyPublicKeysEntry> cache;
    private final ConcurrentMap<String, CompletableFuture<ValkeyPublicKeysEntry>> tasksInProgress;
    private final int minTimeBetweenRequests;
    private final int maxCacheTime;
    private final Set<String> invalidations = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean afterCompletionRegistered = new AtomicBoolean(false);

    ValkeyPublicKeyStorageProvider(KeycloakSession session,
            ConcurrentMap<String, ValkeyPublicKeysEntry> cache,
            ConcurrentMap<String, CompletableFuture<ValkeyPublicKeysEntry>> tasksInProgress,
            int minTimeBetweenRequests,
            int maxCacheTime) {
        this.session = Objects.requireNonNull(session, "session");
        this.cache = Objects.requireNonNull(cache, "cache");
        this.tasksInProgress = Objects.requireNonNull(tasksInProgress, "tasksInProgress");
        this.minTimeBetweenRequests = minTimeBetweenRequests;
        this.maxCacheTime = maxCacheTime;
    }

    @Override
    public KeyWrapper getPublicKey(String modelKey, String kid, String algorithm, PublicKeyLoader loader) {
        ValkeyPublicKeysEntry entry = cache.get(modelKey);
        int currentTime = Time.currentTime();
        int lastRequestTime = entry == null ? 0 : entry.getLastRequestTime();
        boolean canSendRequest = entry == null || currentTime > lastRequestTime + minTimeBetweenRequests;

        if (!isExpired(entry, currentTime) && (!canSendRequest || kid != null)) {
            KeyWrapper key = entry.getKeys().getKeyByKidAndAlg(kid, algorithm);
            if (key != null) {
                return key.cloneKey();
            }
        }

        ValkeyPublicKeysEntry reloaded = reloadKeys(modelKey, entry, loader);
        entry = reloaded == null ? entry : reloaded;
        KeyWrapper key = entry == null ? null : entry.getKeys().getKeyByKidAndAlg(kid, algorithm);
        if (key != null) {
            return key.cloneKey();
        }

        List<String> availableKids = entry == null ? Collections.emptyList() : entry.getKeys().getKids();
        logger.warnf("PublicKey wasn't found in the storage. Requested kid: '%s'. Available kids: '%s'", kid, availableKids);
        return null;
    }

    @Override
    public KeyWrapper getFirstPublicKey(String modelKey, String algorithm, PublicKeyLoader loader) {
        return getPublicKey(modelKey, null, algorithm, loader);
    }

    @Override
    public KeyWrapper getFirstPublicKey(String modelKey, Predicate<KeyWrapper> predicate, PublicKeyLoader loader) {
        ValkeyPublicKeysEntry entry = cache.get(modelKey);
        int currentTime = Time.currentTime();
        if (!isExpired(entry, currentTime) && entry != null) {
            KeyWrapper key = entry.getKeys().getKeyByPredicate(predicate);
            if (key != null) {
                return key.cloneKey();
            }
        }
        entry = reloadKeys(modelKey, entry, loader);
        if (entry != null) {
            KeyWrapper key = entry.getKeys().getKeyByPredicate(predicate);
            if (key != null) {
                return key.cloneKey();
            }
        }
        return null;
    }

    @Override
    public List<KeyWrapper> getKeys(String modelKey, PublicKeyLoader loader) {
        ValkeyPublicKeysEntry entry = cache.get(modelKey);
        int currentTime = Time.currentTime();

        if (isExpired(entry, currentTime)
                || (hasNoExpiration(entry) && entry != null && currentTime > entry.getLastRequestTime() + maxCacheTime)) {
            ValkeyPublicKeysEntry refreshed = reloadKeys(modelKey, entry, loader);
            if (refreshed != null) {
                entry = refreshed;
            }
        }

        if (entry == null) {
            return Collections.emptyList();
        }
        return entry.getKeys().getKeys().stream().map(KeyWrapper::cloneKey).collect(Collectors.toList());
    }

    @Override
    public boolean reloadKeys(String modelKey, PublicKeyLoader loader) {
        return reloadKeys(modelKey, cache.get(modelKey), loader) != null;
    }

    void addInvalidation(String cacheKey) {
        invalidations.add(cacheKey);
        if (afterCompletionRegistered.compareAndSet(false, true)) {
            KeycloakTransactionManager transactionManager = session.getTransactionManager();
            transactionManager.enlistAfterCompletion(createAfterTransaction());
        }
    }

    private KeycloakTransaction createAfterTransaction() {
        return new KeycloakTransaction() {
            @Override
            public void begin() {
            }

            @Override
            public void commit() {
                runInvalidations();
            }

            @Override
            public void rollback() {
                runInvalidations();
            }

            @Override
            public void setRollbackOnly() {
            }

            @Override
            public boolean getRollbackOnly() {
                return false;
            }

            @Override
            public boolean isActive() {
                return true;
            }
        };
    }

    private void runInvalidations() {
        Collection<String> targets = new ArrayList<>(invalidations);
        invalidations.clear();
        afterCompletionRegistered.set(false);
        if (targets.isEmpty()) {
            return;
        }

        targets.forEach(cache::remove);
        ClusterProvider cluster = session.getProvider(ClusterProvider.class);
        if (cluster != null) {
            List<ValkeyPublicKeyInvalidationEvent> events = targets.stream()
                    .map(ValkeyPublicKeyInvalidationEvent::create)
                    .collect(Collectors.toList());
            cluster.notify(ValkeyPublicKeyStorageProviderFactory.PUBLIC_KEY_STORAGE_INVALIDATION_EVENT, events, true);
        }
    }

    private boolean hasNoExpiration(ValkeyPublicKeysEntry entry) {
        return entry == null || entry.getKeys().getExpirationTime() == null;
    }

    private boolean isExpired(ValkeyPublicKeysEntry entry, int currentTime) {
        if (entry == null) {
            return true;
        }
        Long expiration = entry.getKeys().getExpirationTime();
        if (expiration != null) {
            return currentTime > expiration / 1000;
        }
        return false;
    }

    private ValkeyPublicKeysEntry reloadKeys(String modelKey, ValkeyPublicKeysEntry existingEntry, PublicKeyLoader loader) {
        int currentTime = Time.currentTime();
        if (existingEntry != null && currentTime <= existingEntry.getLastRequestTime() + minTimeBetweenRequests) {
            logger.warnf("Won't load the keys for model '%s'. Last request time was %d", modelKey,
                    existingEntry.getLastRequestTime());
            return null;
        }

        CompletableFuture<ValkeyPublicKeysEntry> task = new CompletableFuture<>();
        CompletableFuture<ValkeyPublicKeysEntry> existing = tasksInProgress.putIfAbsent(modelKey, task);
        if (existing == null) {
            try {
                ValkeyPublicKeysEntry loaded = loadKeys(modelKey, loader);
                task.complete(loaded);
                return loaded;
            } catch (RuntimeException ex) {
                task.completeExceptionally(ex);
                throw ex;
            } catch (Exception ex) {
                task.completeExceptionally(ex);
                throw new RuntimeException("Error when loading public keys: " + ex.getMessage(), ex);
            } finally {
                tasksInProgress.remove(modelKey, task);
            }
        }

        try {
            return existing.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Error. Interrupted when loading public keys", ex);
        } catch (ExecutionException ex) {
            throw new RuntimeException("Error when loading public keys: " + ex.getMessage(), ex.getCause());
        }
    }

    private ValkeyPublicKeysEntry loadKeys(String modelKey, PublicKeyLoader loader) throws Exception {
        while (true) {
            ValkeyPublicKeysEntry entry = cache.get(modelKey);
            int lastRequestTime = entry == null ? 0 : entry.getLastRequestTime();
            int currentTime = Time.currentTime();
            if (entry != null && currentTime <= lastRequestTime + minTimeBetweenRequests) {
                return entry;
            }

            PublicKeysWrapper keys = loader.loadKeys();
            if (logger.isDebugEnabled()) {
                logger.debugf("Public keys retrieved successfully for model %s. New kids: %s", modelKey, keys.getKids());
            }
            ValkeyPublicKeysEntry newEntry = new ValkeyPublicKeysEntry(currentTime, keys);
            cache.put(modelKey, newEntry);
            return newEntry;
        }
    }

    @Override
    public void close() {
    }
}
