package org.keycloak.valkey.config;

import java.time.Duration;
import java.util.Objects;
import java.util.regex.Pattern;

import org.keycloak.Config;

/**
 * Configuration model for the Valkey-backed {@code ClusterProvider} implementation.
 */
public class ValkeyClusterConfig {

    private static final Pattern NAMESPACE_PATTERN = Pattern.compile("[a-zA-Z0-9:-]+");
    private static final Duration DEFAULT_COMPLETION_POLL_INTERVAL = Duration.ofMillis(100);
    private static final String DEFAULT_NAMESPACE = "keycloak:cluster";
    private static final String DEFAULT_CHANNEL_SUFFIX = ":events";

    private final String namespace;
    private final Duration completionPollInterval;
    private final String siteName;

    private final String startupKey;
    private final String taskPrefix;
    private final String channel;

    public ValkeyClusterConfig(String namespace, Duration completionPollInterval) {
        this(namespace, completionPollInterval, null);
    }

    public ValkeyClusterConfig(String namespace, Duration completionPollInterval, String siteName) {
        this.namespace = validateNamespace(namespace);
        this.completionPollInterval = requirePositive(completionPollInterval, "completionPollInterval");
        this.siteName = trimToNull(siteName);
        this.startupKey = this.namespace + ":startup";
        this.taskPrefix = this.namespace + ":task:";
        this.channel = this.namespace + DEFAULT_CHANNEL_SUFFIX;
    }

    public String getNamespace() {
        return namespace;
    }

    public Duration getCompletionPollInterval() {
        return completionPollInterval;
    }

    public String getSiteName() {
        return siteName;
    }

    public String startupKey() {
        return startupKey;
    }

    public String taskKey(String taskKey) {
        return taskPrefix + Objects.requireNonNull(taskKey, "taskKey");
    }

    public String getChannel() {
        return channel;
    }

    public static ValkeyClusterConfig from(Config.Scope scope) {
        Config.Scope effective = scope != null ? scope : Config.scope("cluster", "valkey");
        String namespace = trimToNull(effective.get("namespace"));
        if (namespace == null) {
            namespace = DEFAULT_NAMESPACE;
        }
        Duration pollInterval = readDuration(effective, "completion-poll-interval", DEFAULT_COMPLETION_POLL_INTERVAL);
        String siteName = trimToNull(effective.get("site"));
        return new ValkeyClusterConfig(namespace, pollInterval, siteName);
    }

    private static Duration readDuration(Config.Scope scope, String key, Duration defaultValue) {
        String value = trimToNull(scope.get(key));
        if (value == null) {
            return defaultValue;
        }
        try {
            if (isDigits(value)) {
                return Duration.ofMillis(Long.parseLong(value));
            }
            return Duration.parse(value);
        } catch (RuntimeException ex) {
            throw new IllegalArgumentException("Invalid duration for key '" + key + "': " + value, ex);
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
        String candidate = Objects.requireNonNull(trimToNull(namespace), "namespace");
        if (!NAMESPACE_PATTERN.matcher(candidate).matches()) {
            throw new IllegalArgumentException("Namespace must match pattern '" + NAMESPACE_PATTERN.pattern() + "': " + candidate);
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
