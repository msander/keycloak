package org.keycloak.valkey.authsession;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.keycloak.Config;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.provider.Provider;
import org.keycloak.provider.ProviderConfigProperty;
import org.keycloak.provider.ProviderConfigurationBuilder;
import org.keycloak.provider.ServerInfoAwareProviderFactory;
import org.keycloak.sessions.AuthenticationSessionProvider;
import org.keycloak.sessions.AuthenticationSessionProviderFactory;
import org.keycloak.valkey.ValkeyConnectionProvider;

/**
 * Factory for the Valkey-backed {@link AuthenticationSessionProvider} implementation.
 */
public class ValkeyAuthenticationSessionProviderFactory implements
        AuthenticationSessionProviderFactory<ValkeyAuthenticationSessionProvider>, ServerInfoAwareProviderFactory {

    public static final String PROVIDER_ID = "valkey";
    private static final String CONFIG_AUTH_SESSIONS_LIMIT = "authSessionsLimit";
    private static final int DEFAULT_AUTH_SESSIONS_LIMIT = 300;

    private int authSessionsLimit = DEFAULT_AUTH_SESSIONS_LIMIT;

    @Override
    public ValkeyAuthenticationSessionProvider create(KeycloakSession session) {
        ValkeyConnectionProvider connectionProvider = session.getProvider(ValkeyConnectionProvider.class);
        if (connectionProvider == null) {
            throw new IllegalStateException("ValkeyConnectionProvider is required for Valkey authentication sessions");
        }
        return new ValkeyAuthenticationSessionProvider(session, connectionProvider, authSessionsLimit);
    }

    @Override
    public void init(Config.Scope config) {
        if (config == null) {
            return;
        }
        Integer configuredLimit = config.getInt(CONFIG_AUTH_SESSIONS_LIMIT);
        if (configuredLimit != null) {
            authSessionsLimit = configuredLimit > 0 ? configuredLimit : DEFAULT_AUTH_SESSIONS_LIMIT;
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
        return Map.of(CONFIG_AUTH_SESSIONS_LIMIT, Integer.toString(authSessionsLimit));
    }

    @Override
    public Set<Class<? extends Provider>> dependsOn() {
        return Set.of(ValkeyConnectionProvider.class);
    }

    @Override
    public List<ProviderConfigProperty> getConfigMetadata() {
        ProviderConfigurationBuilder builder = ProviderConfigurationBuilder.create();
        builder.property()
                .name(CONFIG_AUTH_SESSIONS_LIMIT)
                .type(ProviderConfigProperty.STRING_TYPE)
                .defaultValue(Integer.toString(DEFAULT_AUTH_SESSIONS_LIMIT))
                .helpText("Maximum number of concurrent authentication sessions per root session when stored in Valkey")
                .add();
        return builder.build();
    }
}
