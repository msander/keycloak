package org.keycloak.valkey.singleuse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.keycloak.Config;
import org.keycloak.common.util.Time;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.SingleUseObjectProvider;
import org.keycloak.models.session.RevokedToken;
import org.keycloak.models.session.RevokedTokenPersisterProvider;
import org.keycloak.provider.ProviderConfigProperty;
import org.keycloak.valkey.ValkeyConnectionProvider;
import org.keycloak.valkey.connection.LettuceValkeyConnectionProviderFactory;
import org.keycloak.valkey.testing.EmbeddedValkeyServer;
import org.keycloak.valkey.testing.MapBackedConfigScope;

import io.lettuce.core.api.StatefulRedisConnection;

class ValkeySingleUseObjectProviderFactoryTest {

    private static EmbeddedValkeyServer server;
    private static LettuceValkeyConnectionProviderFactory connectionFactory;

    @BeforeAll
    static void setupCluster() {
        server = EmbeddedValkeyServer.builder()
                .startupTimeout(Duration.ofSeconds(5))
                .start();
        connectionFactory = new LettuceValkeyConnectionProviderFactory();
        Map<String, String> configValues = new HashMap<>();
        configValues.put("uri", "redis://" + server.getHost() + ":" + server.getPort());
        connectionFactory.init(MapBackedConfigScope.from(configValues));
    }

    @AfterAll
    static void tearDownCluster() {
        if (connectionFactory != null) {
            connectionFactory.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    void createShouldRequireValkeyConnectionProvider() {
        ValkeySingleUseObjectProviderFactory factory = new ValkeySingleUseObjectProviderFactory();
        KeycloakSession session = mock(KeycloakSession.class);
        when(session.getProvider(eq(ValkeyConnectionProvider.class))).thenReturn(null);

        assertThrows(IllegalStateException.class, () -> factory.create(session));
    }

    @Test
    void createShouldLoadRevokedTokensWhenConfigured() {
        ValkeySingleUseObjectProviderFactory factory = new ValkeySingleUseObjectProviderFactory();
        Config.Scope scope = MapBackedConfigScope.from(Map.of(ValkeySingleUseObjectProviderFactory.CONFIG_PERSIST_REVOKED_TOKENS,
                Boolean.TRUE.toString()));
        factory.init(scope);

        ValkeyConnectionProvider connectionProvider = connectionFactory.create(null);
        KeycloakSession session = mock(KeycloakSession.class);
        when(session.getProvider(eq(ValkeyConnectionProvider.class))).thenReturn(connectionProvider);

        RevokedToken token = new RevokedToken("tok", Time.currentTime() + 60);
        RevokedTokenPersisterProvider revokedTokenPersister = mock(RevokedTokenPersisterProvider.class);
        when(session.getProvider(eq(RevokedTokenPersisterProvider.class))).thenReturn(revokedTokenPersister);
        when(revokedTokenPersister.getAllRevokedTokens()).thenReturn(Stream.of(token));

        ValkeySingleUseObjectProvider provider = factory.create(session);
        assertNotNull(provider);

        StatefulRedisConnection<String, String> connection = connectionProvider.getConnection();
        assertTrue(connection.sync().exists(token.tokenId() + SingleUseObjectProvider.REVOKED_KEY) > 0);
        assertEquals("{}", connection.sync().get(ValkeySingleUseObjectProviderFactoryTestHelper.loadedKey()));

        provider.close();
        connectionProvider.close();
    }

    @Test
    void configMetadataShouldExposePersistOption() {
        ValkeySingleUseObjectProviderFactory factory = new ValkeySingleUseObjectProviderFactory();
        List<ProviderConfigProperty> metadata = factory.getConfigMetadata();
        assertEquals(1, metadata.size());
        assertEquals(ValkeySingleUseObjectProviderFactory.CONFIG_PERSIST_REVOKED_TOKENS, metadata.get(0).getName());
    }

    private static final class ValkeySingleUseObjectProviderFactoryTestHelper {
        private ValkeySingleUseObjectProviderFactoryTestHelper() {
        }

        static String loadedKey() {
            return "loaded" + SingleUseObjectProvider.REVOKED_KEY;
        }
    }
}
