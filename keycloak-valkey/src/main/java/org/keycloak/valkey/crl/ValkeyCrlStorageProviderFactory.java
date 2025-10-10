package org.keycloak.valkey.crl;

import java.security.cert.X509CRL;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.keycloak.Config;
import org.keycloak.crl.CrlStorageProvider;
import org.keycloak.crl.CrlStorageProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.provider.ProviderConfigProperty;
import org.keycloak.provider.ProviderConfigurationBuilder;
import org.keycloak.valkey.ValkeyConnectionProvider;

/**
 * Factory for Valkey-backed CRL storage provider.
 */
public class ValkeyCrlStorageProviderFactory implements CrlStorageProviderFactory {

    public static final String PROVIDER_ID = "valkey";

    private final ConcurrentMap<String, CompletableFuture<X509CRL>> tasksInProgress = new ConcurrentHashMap<>();
    private volatile ValkeyCrlStorageConfig config = ValkeyCrlStorageConfig.from(null);

    @Override
    public CrlStorageProvider create(KeycloakSession session) {
        ValkeyConnectionProvider connectionProvider = session.getProvider(ValkeyConnectionProvider.class);
        if (connectionProvider == null) {
            throw new IllegalStateException("ValkeyConnectionProvider is required for CRL storage");
        }
        return new ValkeyCrlStorageProvider(connectionProvider, config, tasksInProgress);
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
        tasksInProgress.clear();
    }

    @Override
    public String getId() {
        return PROVIDER_ID;
    }

    @Override
    public List<ProviderConfigProperty> getConfigMetadata() {
        return ProviderConfigurationBuilder.create()
                .property()
                    .name("namespace")
                    .type(ProviderConfigProperty.STRING_TYPE)
                    .defaultValue(ValkeyCrlStorageConfig.DEFAULT_NAMESPACE)
                    .helpText("Namespace prefix for storing CRL entries in Valkey.")
                    .add()
                .property()
                    .name("cacheTime")
                    .type(ProviderConfigProperty.STRING_TYPE)
                    .defaultValue("-1")
                    .helpText("Interval in seconds that the CRL remains cached. A non-positive value defers to the CRL next update.")
                    .add()
                .property()
                    .name("minTimeBetweenRequests")
                    .type(ProviderConfigProperty.STRING_TYPE)
                    .defaultValue("10")
                    .helpText("Minimum interval in seconds between CRL reload attempts.")
                    .add()
                .build();
    }
}
