package org.keycloak.valkey.usersession;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.models.UserSessionProvider;
import org.keycloak.models.UserSessionProviderFactory;
import org.keycloak.provider.Provider;
import org.keycloak.provider.ProviderConfigProperty;
import org.keycloak.provider.ProviderConfigurationBuilder;
import org.keycloak.provider.ServerInfoAwareProviderFactory;
import org.keycloak.valkey.ValkeyConnectionProvider;

/**
 * Factory for the Valkey-backed {@link UserSessionProvider} implementation.
 */
public class ValkeyUserSessionProviderFactory implements
        UserSessionProviderFactory<ValkeyUserSessionProvider>, ServerInfoAwareProviderFactory {

    public static final String PROVIDER_ID = "valkey";

    @Override
    public ValkeyUserSessionProvider create(KeycloakSession session) {
        ValkeyConnectionProvider connectionProvider = session.getProvider(ValkeyConnectionProvider.class);
        if (connectionProvider == null) {
            throw new IllegalStateException("ValkeyConnectionProvider is required for Valkey user sessions");
        }
        return new ValkeyUserSessionProvider(session, connectionProvider);
    }

    @Override
    public void init(org.keycloak.Config.Scope config) {
        // No custom configuration yet
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
        return Map.of();
    }

    @Override
    public Set<Class<? extends Provider>> dependsOn() {
        return Set.of(ValkeyConnectionProvider.class);
    }

    @Override
    public List<ProviderConfigProperty> getConfigMetadata() {
        return ProviderConfigurationBuilder.create().build();
    }
}
