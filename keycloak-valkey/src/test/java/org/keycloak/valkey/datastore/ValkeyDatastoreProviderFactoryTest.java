package org.keycloak.valkey.datastore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.keycloak.cluster.ClusterProvider;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.models.dblock.DBLockProvider;
import org.keycloak.provider.ProviderFactory;
import org.keycloak.storage.DatastoreProvider;
import org.keycloak.storage.datastore.DefaultDatastoreProvider;
import org.keycloak.valkey.ValkeyConnectionProvider;
import org.keycloak.valkey.cluster.ValkeyClusterProviderFactory;
import org.keycloak.valkey.dblock.ValkeyDBLockProviderFactory;

class ValkeyDatastoreProviderFactoryTest {

    private final ValkeyDatastoreProviderFactory factory = new ValkeyDatastoreProviderFactory();

    @Test
    void createRequiresValkeyConnectionProvider() {
        KeycloakSession session = mock(KeycloakSession.class);
        when(session.getProvider(ValkeyConnectionProvider.class)).thenReturn(null);
        when(session.getKeycloakSessionFactory()).thenReturn(mock(KeycloakSessionFactory.class));

        assertThrows(IllegalStateException.class, () -> factory.create(session));
    }

    @Test
    void createRequiresClusterFactory() {
        KeycloakSession session = mock(KeycloakSession.class);
        KeycloakSessionFactory sessionFactory = mock(KeycloakSessionFactory.class);
        when(session.getProvider(ValkeyConnectionProvider.class)).thenReturn(mock(ValkeyConnectionProvider.class));
        when(session.getKeycloakSessionFactory()).thenReturn(sessionFactory);
        when(sessionFactory.getProviderFactory(eq(ClusterProvider.class), eq(ValkeyClusterProviderFactory.PROVIDER_ID)))
                .thenReturn(null);
        when(sessionFactory.getProviderFactory(eq(DBLockProvider.class), anyString()))
                .thenReturn(mock(ProviderFactory.class));

        assertThrows(IllegalStateException.class, () -> factory.create(session));
    }

    @Test
    void createRequiresDbLockFactory() {
        KeycloakSession session = mock(KeycloakSession.class);
        KeycloakSessionFactory sessionFactory = mock(KeycloakSessionFactory.class);
        when(session.getProvider(ValkeyConnectionProvider.class)).thenReturn(mock(ValkeyConnectionProvider.class));
        when(session.getKeycloakSessionFactory()).thenReturn(sessionFactory);
        when(sessionFactory.getProviderFactory(eq(ClusterProvider.class), eq(ValkeyClusterProviderFactory.PROVIDER_ID)))
                .thenReturn(mock(ProviderFactory.class));
        when(sessionFactory.getProviderFactory(eq(DBLockProvider.class), eq(ValkeyDBLockProviderFactory.PROVIDER_ID)))
                .thenReturn(null);

        assertThrows(IllegalStateException.class, () -> factory.create(session));
    }

    @Test
    void createWrapsDefaultProviderWhenDependenciesPresent() {
        KeycloakSession session = mock(KeycloakSession.class);
        KeycloakSessionFactory sessionFactory = mock(KeycloakSessionFactory.class);
        when(session.getProvider(ValkeyConnectionProvider.class)).thenReturn(mock(ValkeyConnectionProvider.class));
        when(session.getKeycloakSessionFactory()).thenReturn(sessionFactory);
        when(sessionFactory.getProviderFactory(eq(ClusterProvider.class), eq(ValkeyClusterProviderFactory.PROVIDER_ID)))
                .thenReturn(mock(ProviderFactory.class));
        when(sessionFactory.getProviderFactory(eq(DBLockProvider.class), eq(ValkeyDBLockProviderFactory.PROVIDER_ID)))
                .thenReturn(mock(ProviderFactory.class));

        DatastoreProvider provider = factory.create(session);
        assertTrue(provider instanceof ValkeyDatastoreProvider);
        assertTrue(provider instanceof DefaultDatastoreProvider);
    }

    @Test
    void exposesValkeyProviderId() {
        assertEquals(ValkeyDatastoreProviderFactory.PROVIDER_ID, factory.getId());
    }
}
