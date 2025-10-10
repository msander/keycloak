package org.keycloak.valkey.loginfailure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.keycloak.models.KeycloakSession;
import org.keycloak.provider.Provider;
import org.keycloak.provider.ProviderConfigProperty;
import org.keycloak.valkey.ValkeyConnectionProvider;
import org.keycloak.valkey.connection.LettuceValkeyConnectionProviderFactory;
import org.keycloak.valkey.testing.EmbeddedValkeyServer;
import org.keycloak.valkey.testing.MapBackedConfigScope;

class ValkeyUserLoginFailureProviderFactoryTest {

    private static EmbeddedValkeyServer server;
    private static LettuceValkeyConnectionProviderFactory connectionFactory;

    private ValkeyConnectionProvider connectionProvider;
    private KeycloakSession session;

    @BeforeAll
    static void startServer() {
        server = EmbeddedValkeyServer.builder()
                .startupTimeout(Duration.ofSeconds(5))
                .start();
        connectionFactory = new LettuceValkeyConnectionProviderFactory();
        Map<String, String> configValues = new HashMap<>();
        configValues.put("uri", "redis://" + server.getHost() + ":" + server.getPort());
        connectionFactory.init(MapBackedConfigScope.from(configValues));
    }

    @AfterAll
    static void shutdown() {
        if (connectionFactory != null) {
            connectionFactory.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @BeforeEach
    void setUp() {
        connectionProvider = connectionFactory.create(null);
        session = mock(KeycloakSession.class);
        when(session.getProvider(eq(ValkeyConnectionProvider.class))).thenReturn(connectionProvider);
    }

    @AfterEach
    void tearDown() {
        if (connectionProvider != null) {
            connectionProvider.close();
        }
    }

    @Test
    void createShouldFailWithoutConnectionProvider() {
        ValkeyUserLoginFailureProviderFactory factory = new ValkeyUserLoginFailureProviderFactory();
        KeycloakSession missingConnection = mock(KeycloakSession.class);
        assertThrows(IllegalStateException.class, () -> factory.create(missingConnection));
    }

    @Test
    void shouldExposeOperationalInfo() {
        ValkeyUserLoginFailureProviderFactory factory = new ValkeyUserLoginFailureProviderFactory();
        Map<String, String> config = Map.of(
                "namespace", "custom:login-failure",
                "min-lifespan", "3600");
        factory.init(MapBackedConfigScope.from(config));

        Map<String, String> info = factory.getOperationalInfo();
        assertEquals("custom:login-failure", info.get("namespace"));
        assertEquals("3600", info.get("min-lifespan"));
    }

    @Test
    void dependsOnShouldIncludeValkeyConnectionProvider() {
        ValkeyUserLoginFailureProviderFactory factory = new ValkeyUserLoginFailureProviderFactory();
        Set<Class<? extends Provider>> dependencies = factory.dependsOn();
        assertTrue(dependencies.contains(ValkeyConnectionProvider.class));
    }

    @Test
    void configMetadataShouldDescribeProperties() {
        ValkeyUserLoginFailureProviderFactory factory = new ValkeyUserLoginFailureProviderFactory();
        List<ProviderConfigProperty> metadata = factory.getConfigMetadata();
        assertEquals(2, metadata.size());
        verifyProperty(metadata, "namespace");
        verifyProperty(metadata, "min-lifespan");
    }

    @Test
    void createShouldReturnProviderInstance() {
        ValkeyUserLoginFailureProviderFactory factory = new ValkeyUserLoginFailureProviderFactory();
        factory.init(MapBackedConfigScope.from(Map.of("namespace", "factory:test")));

        ValkeyUserLoginFailureProvider provider = factory.create(session);
        assertEquals("valkey", factory.getId());
        provider.close();
        verify(session).getProvider(ValkeyConnectionProvider.class);
    }

    private void verifyProperty(List<ProviderConfigProperty> metadata, String name) {
        boolean found = metadata.stream().anyMatch(property -> name.equals(property.getName()));
        assertTrue(found, () -> "Expected config property " + name);
    }
}
