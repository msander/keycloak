package org.keycloak.valkey.datastore;

import org.keycloak.cluster.ClusterProvider;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.models.dblock.DBLockProvider;
import org.keycloak.provider.ProviderFactory;
import org.keycloak.storage.DatastoreProvider;
import org.keycloak.storage.datastore.DefaultDatastoreProviderFactory;
import org.keycloak.valkey.ValkeyConnectionProvider;
import org.keycloak.valkey.cluster.ValkeyClusterProviderFactory;
import org.keycloak.valkey.dblock.ValkeyDBLockProviderFactory;

/**
 * Datastore factory that ensures Valkey-backed providers are preferred where available.
 */
public class ValkeyDatastoreProviderFactory extends DefaultDatastoreProviderFactory {

    public static final String PROVIDER_ID = "valkey";

    @Override
    public DatastoreProvider create(KeycloakSession session) {
        ensureValkeyInfrastructure(session);
        return new ValkeyDatastoreProvider(this, session);
    }

    private void ensureValkeyInfrastructure(KeycloakSession session) {
        if (session.getProvider(ValkeyConnectionProvider.class) == null) {
            throw new IllegalStateException("Valkey datastore requires an active Valkey connection provider");
        }
        KeycloakSessionFactory sessionFactory = session.getKeycloakSessionFactory();
        if (sessionFactory == null) {
            throw new IllegalStateException("KeycloakSessionFactory is not available for Valkey datastore initialisation");
        }
        requireFactory(sessionFactory, ClusterProvider.class, ValkeyClusterProviderFactory.PROVIDER_ID,
                "Valkey cluster provider must be enabled when using the Valkey datastore");
        requireFactory(sessionFactory, DBLockProvider.class, ValkeyDBLockProviderFactory.PROVIDER_ID,
                "Valkey DB lock provider must be enabled when using the Valkey datastore");
    }

    private <T extends org.keycloak.provider.Provider> void requireFactory(KeycloakSessionFactory sessionFactory,
            Class<T> providerClass, String providerId, String message) {
        ProviderFactory<T> factory = sessionFactory.getProviderFactory(providerClass, providerId);
        if (factory == null) {
            throw new IllegalStateException(message);
        }
    }

    @Override
    public String getId() {
        return PROVIDER_ID;
    }
}
