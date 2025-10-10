package org.keycloak.valkey.loginfailure;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.keycloak.Config;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.models.UserLoginFailureProvider;
import org.keycloak.models.UserLoginFailureProviderFactory;
import org.keycloak.provider.Provider;
import org.keycloak.provider.ProviderConfigProperty;
import org.keycloak.provider.ProviderConfigurationBuilder;
import org.keycloak.provider.ServerInfoAwareProviderFactory;
import org.keycloak.valkey.ValkeyConnectionProvider;

/**
 * Factory that wires the Valkey-backed {@link UserLoginFailureProvider} implementation.
 */
public class ValkeyUserLoginFailureProviderFactory
        implements UserLoginFailureProviderFactory<ValkeyUserLoginFailureProvider>, ServerInfoAwareProviderFactory {

    public static final String PROVIDER_ID = "valkey";
    private static final String CONFIG_NAMESPACE = "namespace";
    private static final String CONFIG_MIN_LIFESPAN = "min-lifespan";
    private static final Duration DEFAULT_MIN_LIFESPAN = Duration.ofHours(12);
    private static final String DEFAULT_NAMESPACE = "kc:login-failure";

    private String namespace = DEFAULT_NAMESPACE;
    private long minimumLifespanSeconds = DEFAULT_MIN_LIFESPAN.toSeconds();

    @Override
    public ValkeyUserLoginFailureProvider create(KeycloakSession session) {
        ValkeyConnectionProvider connectionProvider = session.getProvider(ValkeyConnectionProvider.class);
        if (connectionProvider == null) {
            throw new IllegalStateException("ValkeyConnectionProvider is required for Valkey login failure provider");
        }
        return new ValkeyUserLoginFailureProvider(session, connectionProvider, namespace, minimumLifespanSeconds);
    }

    @Override
    public void init(Config.Scope config) {
        if (config == null) {
            return;
        }
        String configuredNamespace = trimToNull(config.get(CONFIG_NAMESPACE));
        if (configuredNamespace != null) {
            this.namespace = configuredNamespace;
        }
        String minLifespan = trimToNull(config.get(CONFIG_MIN_LIFESPAN));
        if (minLifespan != null) {
            this.minimumLifespanSeconds = parseLifespan(minLifespan);
        }
        if (minimumLifespanSeconds <= 0) {
            throw new IllegalArgumentException("min-lifespan must be positive");
        }
    }

    @Override
    public void postInit(KeycloakSessionFactory factory) {
        // No-op
    }

    @Override
    public void close() {
        // Nothing to close
    }

    @Override
    public String getId() {
        return PROVIDER_ID;
    }

    @Override
    public Map<String, String> getOperationalInfo() {
        return Map.of(
                CONFIG_NAMESPACE, namespace,
                CONFIG_MIN_LIFESPAN, Long.toString(minimumLifespanSeconds));
    }

    @Override
    public Set<Class<? extends Provider>> dependsOn() {
        return Set.of(ValkeyConnectionProvider.class);
    }

    @Override
    public List<ProviderConfigProperty> getConfigMetadata() {
        ProviderConfigurationBuilder builder = ProviderConfigurationBuilder.create();
        builder.property()
                .name(CONFIG_NAMESPACE)
                .type(ProviderConfigProperty.STRING_TYPE)
                .defaultValue(DEFAULT_NAMESPACE)
                .helpText("Namespace prefix applied to login failure keys stored in Valkey")
                .add();
        builder.property()
                .name(CONFIG_MIN_LIFESPAN)
                .type(ProviderConfigProperty.STRING_TYPE)
                .defaultValue(DEFAULT_MIN_LIFESPAN.toString())
                .helpText("Minimum TTL applied to login failure records when updating Valkey")
                .add();
        return builder.build();
    }

    private static long parseLifespan(String candidate) {
        Objects.requireNonNull(candidate, "min-lifespan");
        try {
            if (candidate.chars().allMatch(Character::isDigit)) {
                return Long.parseLong(candidate);
            }
            Duration duration = Duration.parse(candidate);
            return duration.isNegative() ? -1 : Math.max(1, duration.getSeconds());
        } catch (RuntimeException ex) {
            throw new IllegalArgumentException("Invalid min-lifespan value: " + candidate, ex);
        }
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
