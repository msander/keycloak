package org.keycloak.valkey.config;

import java.time.Duration;
import java.util.Objects;

import org.keycloak.Config;

import io.lettuce.core.RedisURI;

/**
 * Immutable configuration model describing how the Valkey connection factory should behave.
 */
public class ValkeyConfig {

    public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
    public static final Duration DEFAULT_COMMAND_TIMEOUT = Duration.ofSeconds(5);
    public static final Duration DEFAULT_SHUTDOWN_QUIET_PERIOD = Duration.ofMillis(100);
    public static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(2);
    public static final int DEFAULT_REQUEST_QUEUE_SIZE = 32_768;

    private final String uri;
    private final String clientName;
    private final Duration connectTimeout;
    private final Duration commandTimeout;
    private final Duration shutdownQuietPeriod;
    private final Duration shutdownTimeout;
    private final boolean ssl;
    private final boolean startTls;
    private final boolean verifyPeer;
    private final int requestQueueSize;

    public ValkeyConfig(String uri, String clientName, Duration connectTimeout, Duration commandTimeout,
            Duration shutdownQuietPeriod, Duration shutdownTimeout, boolean ssl, boolean startTls,
            boolean verifyPeer, int requestQueueSize) {
        this.uri = validateUri(uri);
        this.clientName = clientName;
        this.connectTimeout = requirePositive(connectTimeout, "connectTimeout");
        this.commandTimeout = requirePositive(commandTimeout, "commandTimeout");
        this.shutdownQuietPeriod = requireNonNegative(shutdownQuietPeriod, "shutdownQuietPeriod");
        this.shutdownTimeout = requireNonNegative(shutdownTimeout, "shutdownTimeout");
        this.ssl = ssl;
        this.startTls = startTls;
        this.verifyPeer = verifyPeer;
        this.requestQueueSize = validateQueueSize(requestQueueSize);
    }

    public String getUri() {
        return uri;
    }

    public String getClientName() {
        return clientName;
    }

    /**
     * @return the URI configured for connections with credentials masked when present.
     */
    public String getSanitizedUri() {
        return maskUri(uri);
    }

    public Duration getConnectTimeout() {
        return connectTimeout;
    }

    public Duration getCommandTimeout() {
        return commandTimeout;
    }

    public Duration getShutdownQuietPeriod() {
        return shutdownQuietPeriod;
    }

    public Duration getShutdownTimeout() {
        return shutdownTimeout;
    }

    public boolean isSsl() {
        return ssl;
    }

    public boolean isStartTls() {
        return startTls;
    }

    public boolean isVerifyPeer() {
        return verifyPeer;
    }

    public int getRequestQueueSize() {
        return requestQueueSize;
    }

    public RedisURI toRedisURI() {
        RedisURI redisURI = RedisURI.create(uri);
        redisURI.setTimeout(commandTimeout);
        redisURI.setVerifyPeer(verifyPeer);
        if (ssl) {
            redisURI.setSsl(true);
            redisURI.setStartTls(startTls);
        }
        if (clientName != null && !clientName.isBlank()) {
            redisURI.setClientName(clientName);
        }
        return redisURI;
    }

    public static ValkeyConfig from(Config.Scope scope) {
        String uri = trimToNull(scope.get("uri"));
        if (uri == null) {
            uri = "redis://127.0.0.1:6379";
        }

        String clientName = trimToNull(scope.get("client-name"));
        Duration connectTimeout = readDuration(scope, "connect-timeout", DEFAULT_CONNECT_TIMEOUT);
        Duration commandTimeout = readDuration(scope, "command-timeout", DEFAULT_COMMAND_TIMEOUT);
        Duration shutdownQuietPeriod = readDuration(scope, "shutdown-quiet-period", DEFAULT_SHUTDOWN_QUIET_PERIOD);
        Duration shutdownTimeout = readDuration(scope, "shutdown-timeout", DEFAULT_SHUTDOWN_TIMEOUT);
        boolean ssl = scope.getBoolean("ssl", Boolean.FALSE);
        boolean startTls = scope.getBoolean("start-tls", Boolean.FALSE);
        boolean verifyPeer = scope.getBoolean("verify-peer", Boolean.TRUE);
        Integer queueSize = scope.getInt("request-queue-size", DEFAULT_REQUEST_QUEUE_SIZE);

        return new ValkeyConfig(uri, clientName, connectTimeout, commandTimeout, shutdownQuietPeriod,
                shutdownTimeout, ssl, startTls, verifyPeer, queueSize);
    }

    private static Duration readDuration(Config.Scope scope, String key, Duration defaultValue) {
        String value = trimToNull(scope.get(key));
        if (value == null) {
            return defaultValue;
        }
        try {
            if (isOnlyDigits(value)) {
                return Duration.ofMillis(Long.parseLong(value));
            }
            return Duration.parse(value);
        } catch (RuntimeException ex) {
            throw new IllegalArgumentException("Invalid duration for key '" + key + "': " + value, ex);
        }
    }

    private static boolean isOnlyDigits(String candidate) {
        for (int i = 0; i < candidate.length(); i++) {
            if (!Character.isDigit(candidate.charAt(i))) {
                return false;
            }
        }
        return !candidate.isEmpty();
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    @Override
    public String toString() {
        return "ValkeyConfig{" +
                "uri='" + maskUri(uri) + '\'' +
                ", clientName='" + clientName + '\'' +
                ", connectTimeout=" + connectTimeout +
                ", commandTimeout=" + commandTimeout +
                ", shutdownQuietPeriod=" + shutdownQuietPeriod +
                ", shutdownTimeout=" + shutdownTimeout +
                ", ssl=" + ssl +
                ", startTls=" + startTls +
                ", verifyPeer=" + verifyPeer +
                ", requestQueueSize=" + requestQueueSize +
                '}';
    }

    private static String maskUri(String uri) {
        int atIndex = uri.indexOf('@');
        if (atIndex < 0) {
            return uri;
        }
        int schemeIndex = uri.indexOf("://");
        if (schemeIndex < 0 || atIndex < schemeIndex) {
            return uri;
        }
        String scheme = uri.substring(0, schemeIndex + 3);
        String host = uri.substring(atIndex + 1);
        return scheme + "***@" + host;
    }

    private static String validateUri(String uri) {
        String candidate = Objects.requireNonNull(uri, "Valkey URI must not be null");
        if (candidate.isBlank()) {
            throw new IllegalArgumentException("Valkey URI must not be blank");
        }
        return candidate;
    }

    private static Duration requirePositive(Duration duration, String name) {
        Duration value = Objects.requireNonNull(duration, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static Duration requireNonNegative(Duration duration, String name) {
        Duration value = Objects.requireNonNull(duration, name);
        if (value.isNegative()) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return value;
    }

    private static int validateQueueSize(int queueSize) {
        if (queueSize <= 0) {
            throw new IllegalArgumentException("requestQueueSize must be positive");
        }
        return queueSize;
    }
}
