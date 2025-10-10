package org.keycloak.valkey.crl;

import java.io.ByteArrayInputStream;
import java.security.GeneralSecurityException;
import java.security.cert.CRLException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509CRL;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.jboss.logging.Logger;
import org.keycloak.crl.CrlStorageProvider;
import org.keycloak.valkey.ValkeyConnectionProvider;

import io.lettuce.core.SetArgs;
import io.lettuce.core.api.sync.RedisCommands;

final class ValkeyCrlStorageProvider implements CrlStorageProvider {

    private static final Logger logger = Logger.getLogger(ValkeyCrlStorageProvider.class);

    private final ValkeyConnectionProvider connectionProvider;
    private final ValkeyCrlStorageConfig config;
    private final ConcurrentMap<String, CompletableFuture<X509CRL>> tasksInProgress;

    ValkeyCrlStorageProvider(ValkeyConnectionProvider connectionProvider,
            ValkeyCrlStorageConfig config,
            ConcurrentMap<String, CompletableFuture<X509CRL>> tasksInProgress) {
        this.connectionProvider = Objects.requireNonNull(connectionProvider, "connectionProvider");
        this.config = Objects.requireNonNull(config, "config");
        this.tasksInProgress = Objects.requireNonNull(tasksInProgress, "tasksInProgress");
    }

    @Override
    public X509CRL get(String key, java.util.concurrent.Callable<X509CRL> loader) throws GeneralSecurityException {
        LoadedCrl existing = loadCrl(key);
        long now = System.currentTimeMillis();
        if (existing != null && isValid(existing.crl(), now)) {
            logger.debugf("Returning CRL '%s' from Valkey cache", key);
            return existing.crl();
        }
        return reloadCrl(key, loader, now, existing);
    }

    @Override
    public boolean refreshCache(String key, java.util.concurrent.Callable<X509CRL> loader)
            throws GeneralSecurityException {
        LoadedCrl existing = loadCrl(key);
        long now = System.currentTimeMillis();
        X509CRL crl = reloadCrl(key, loader, now, existing);
        return crl != null && (existing == null || existing.crl() != crl);
    }

    @Override
    public void close() {
        // No-op
    }

    private X509CRL reloadCrl(String key, java.util.concurrent.Callable<X509CRL> loader, long now, LoadedCrl existing)
            throws GeneralSecurityException {
        if (existing != null && now < existing.lastRequestTime() + config.minTimeBetweenRequestsMillis()) {
            logger.debugf("Avoiding reload of CRL '%s'; last refresh at %d", key, existing.lastRequestTime());
            return existing.crl();
        }

        CompletableFuture<X509CRL> future = new CompletableFuture<>();
        CompletableFuture<X509CRL> current = tasksInProgress.putIfAbsent(key, future);
        if (current == null) {
            try {
                X509CRL crl = loader.call();
                if (crl == null) {
                    logger.warnf("Loader for CRL '%s' returned null", key);
                    future.complete(null);
                    return null;
                }
                storeRecord(key, crl, now);
                future.complete(crl);
                logger.debugf("Reloaded CRL '%s' into Valkey cache", key);
                return crl;
            } catch (GeneralSecurityException | RuntimeException ex) {
                future.completeExceptionally(ex);
                throw ex;
            } catch (Exception ex) {
                future.completeExceptionally(ex);
                throw new GeneralSecurityException("Error when loading CRL " + key, ex);
            } finally {
                tasksInProgress.remove(key, future);
            }
        } else {
            try {
                return current.get();
            } catch (ExecutionException ex) {
                Throwable cause = ex.getCause();
                if (cause instanceof GeneralSecurityException gse) {
                    throw gse;
                }
                throw new GeneralSecurityException("Error when loading CRL " + key, cause);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new GeneralSecurityException("Interrupted when loading CRL " + key, ex);
            }
        }
    }

    private void storeRecord(String crlKey, X509CRL crl, long now) throws GeneralSecurityException {
        byte[] encoded;
        try {
            encoded = crl.getEncoded();
        } catch (CRLException ex) {
            throw new GeneralSecurityException("Failed to encode CRL " + crlKey, ex);
        }
        ValkeyCrlRecord record = ValkeyCrlRecord.create(encoded, now);
        String payload = record.toJson();
        String redisKey = config.key(crlKey);
        RedisCommands<String, String> commands = connectionProvider.getConnection().sync();
        long lifespan = computeLifespan(crl, now);
        if (lifespan > 0) {
            commands.set(redisKey, payload, SetArgs.Builder.px(lifespan));
        } else {
            commands.set(redisKey, payload);
        }
    }

    private LoadedCrl loadCrl(String crlKey) throws GeneralSecurityException {
        String payload = connectionProvider.getConnection().sync().get(config.key(crlKey));
        ValkeyCrlRecord record = ValkeyCrlRecord.fromJson(payload);
        if (record == null) {
            return null;
        }
        X509CRL crl = decode(record.crlBytes());
        return new LoadedCrl(crl, record.lastRequestTime());
    }

    private X509CRL decode(byte[] data) throws GeneralSecurityException {
        try {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            return (X509CRL) factory.generateCRL(new ByteArrayInputStream(data));
        } catch (CRLException ex) {
            throw new GeneralSecurityException("Failed to decode CRL", ex);
        }
    }

    private boolean isValid(X509CRL crl, long now) {
        Date nextUpdate = crl.getNextUpdate();
        return nextUpdate == null || nextUpdate.getTime() > now;
    }

    private long computeLifespan(X509CRL crl, long now) {
        long cacheTime = config.cacheTimeMillis();
        Date nextUpdate = crl.getNextUpdate();
        if (nextUpdate == null) {
            return cacheTime;
        }
        long nextUpdateDelay = nextUpdate.getTime() - now;
        if (nextUpdateDelay <= 0) {
            return config.minTimeBetweenRequestsMillis();
        }
        if (cacheTime > 0) {
            return Math.min(cacheTime, nextUpdateDelay);
        }
        return nextUpdateDelay;
    }

    private record LoadedCrl(X509CRL crl, long lastRequestTime) {
    }
}
