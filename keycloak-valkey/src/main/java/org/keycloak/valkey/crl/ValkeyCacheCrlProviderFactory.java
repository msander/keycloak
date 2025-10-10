package org.keycloak.valkey.crl;

import org.keycloak.Config;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.models.cache.CacheCrlProvider;
import org.keycloak.models.cache.CacheCrlProviderFactory;
import org.keycloak.valkey.ValkeyConnectionProvider;

/**
 * Factory for the Valkey-backed CacheCrlProvider implementation.
 */
public class ValkeyCacheCrlProviderFactory implements CacheCrlProviderFactory {

    public static final String PROVIDER_ID = "valkey";

    private volatile ValkeyCrlStorageConfig config = ValkeyCrlStorageConfig.from(null);

    @Override
    public CacheCrlProvider create(KeycloakSession session) {
        ValkeyConnectionProvider connectionProvider = session.getProvider(ValkeyConnectionProvider.class);
        if (connectionProvider == null) {
            throw new IllegalStateException("ValkeyConnectionProvider is required for CRL cache operations");
        }
        return new ValkeyCacheCrlProvider(connectionProvider, config);
    }

    @Override
    public void init(Config.Scope scope) {
        this.config = ValkeyCrlStorageConfig.from(scope);
    }

    @Override
    public void postInit(KeycloakSessionFactory factory) {
        // No-op
    }

    @Override
    public void close() {
        // No-op
    }

    @Override
    public String getId() {
        return PROVIDER_ID;
    }
}
