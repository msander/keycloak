package org.keycloak.valkey.config;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.keycloak.Config;
import org.keycloak.models.dblock.DBLockProvider.Namespace;

/**
 * Configuration model for the Valkey-backed {@code DBLockProvider} implementation.
 */
public class ValkeyDBLockConfig {

    private static final Duration DEFAULT_RECHECK_INTERVAL = Duration.ofMillis(500);
    private static final Duration DEFAULT_WAIT_TIMEOUT = Duration.ofSeconds(900);
    private static final Duration DEFAULT_LOCK_LEASE = Duration.ofSeconds(900);
    private static final String DEFAULT_NAMESPACE = "keycloak:dblock";

    private final String namespace;
    private final Duration recheckInterval;
    private final Duration waitTimeout;
    private final Duration lockLease;
    private final boolean forceUnlockOnStartup;
    private final Map<Namespace, String> keys;

    public ValkeyDBLockConfig(String namespace, Duration recheckInterval, Duration waitTimeout,
            Duration lockLease, boolean forceUnlockOnStartup) {
        this.namespace = validateNamespace(namespace);
        this.recheckInterval = requirePositive(recheckInterval, "recheckInterval");
        this.waitTimeout = requirePositive(waitTimeout, "waitTimeout");
        this.lockLease = requirePositive(lockLease, "lockLease");
        if (this.lockLease.compareTo(this.recheckInterval) < 0) {
            throw new IllegalArgumentException("Lock lease must be greater than recheck interval");
        }
        this.forceUnlockOnStartup = forceUnlockOnStartup;
        this.keys = buildKeys(this.namespace);
    }

    public String getNamespace() {
        return namespace;
    }

    public Duration getRecheckInterval() {
        return recheckInterval;
    }

    public Duration getWaitTimeout() {
        return waitTimeout;
    }

    public Duration getLockLease() {
        return lockLease;
    }

    public boolean isForceUnlockOnStartup() {
        return forceUnlockOnStartup;
    }

    public String keyFor(Namespace namespace) {
        String key = keys.get(Objects.requireNonNull(namespace, "namespace"));
        if (key == null) {
            throw new IllegalArgumentException("Unknown namespace: " + namespace);
        }
        return key;
    }

    public String[] allKeys() {
        return keys.values().toArray(String[]::new);
    }

    public ValkeyDBLockConfig withTimeouts(Duration recheckInterval, Duration waitTimeout) {
        return new ValkeyDBLockConfig(namespace, recheckInterval, waitTimeout, lockLease, forceUnlockOnStartup);
    }

    public static ValkeyDBLockConfig from(Config.Scope scope) {
        Config.Scope effective = scope != null ? scope : Config.scope("dblock", "valkey");
        String namespace = trimToNull(effective.get("namespace"));
        if (namespace == null) {
            namespace = DEFAULT_NAMESPACE;
        }
        Duration recheck = readDuration(effective, "lock-recheck-time", DEFAULT_RECHECK_INTERVAL);
        Duration wait = readDuration(effective, "lock-wait-timeout", DEFAULT_WAIT_TIMEOUT);
        Duration lease = readDuration(effective, "lock-lease", DEFAULT_LOCK_LEASE);
        boolean forceUnlock = effective.getBoolean("force-unlock-on-startup", Boolean.FALSE);
        return new ValkeyDBLockConfig(namespace, recheck, wait, lease, forceUnlock);
    }

    private static Map<Namespace, String> buildKeys(String namespace) {
        Map<Namespace, String> keys = new EnumMap<>(Namespace.class);
        for (Namespace ns : Namespace.values()) {
            keys.put(ns, namespace + ':' + ns.name().toLowerCase(Locale.ROOT).replace('_', '-'));
        }
        return keys;
    }

    private static Duration readDuration(Config.Scope scope, String key, Duration defaultValue) {
        String raw = trimToNull(scope.get(key));
        if (raw == null) {
            return defaultValue;
        }
        try {
            if (isDigits(raw)) {
                return Duration.ofMillis(Long.parseLong(raw));
            }
            return Duration.parse(raw);
        } catch (RuntimeException ex) {
            throw new IllegalArgumentException("Invalid duration for key '" + key + "': " + raw, ex);
        }
    }

    private static boolean isDigits(String value) {
        for (int i = 0; i < value.length(); i++) {
            if (!Character.isDigit(value.charAt(i))) {
                return false;
            }
        }
        return !value.isEmpty();
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static String validateNamespace(String namespace) {
        String candidate = trimToNull(namespace);
        if (candidate == null) {
            throw new IllegalArgumentException("Namespace must not be blank");
        }
        for (int i = 0; i < candidate.length(); i++) {
            char ch = candidate.charAt(i);
            if (!(Character.isLetterOrDigit(ch) || ch == ':' || ch == '-' || ch == '_')) {
                throw new IllegalArgumentException("Namespace contains invalid character: " + ch);
            }
        }
        return candidate;
    }

    private static Duration requirePositive(Duration duration, String attribute) {
        Objects.requireNonNull(duration, attribute);
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(attribute + " must be positive");
        }
        return duration;
    }
}
