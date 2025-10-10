package org.keycloak.valkey.singleuse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.ModelException;
import org.keycloak.models.SingleUseObjectProvider;
import org.keycloak.models.session.RevokedTokenPersisterProvider;
import org.keycloak.valkey.ValkeyConnectionProvider;
import org.keycloak.valkey.connection.LettuceValkeyConnectionProviderFactory;
import org.keycloak.valkey.testing.EmbeddedValkeyServer;
import org.keycloak.valkey.testing.MapBackedConfigScope;

import io.lettuce.core.api.StatefulRedisConnection;

class ValkeySingleUseObjectProviderTest {

    private static EmbeddedValkeyServer server;
    private static LettuceValkeyConnectionProviderFactory connectionFactory;

    private ValkeyConnectionProvider connectionProvider;
    private KeycloakSession session;
    private ValkeySingleUseObjectProvider provider;

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
        if (provider != null) {
            provider.close();
        }
        if (connectionProvider != null) {
            connectionProvider.close();
        }
        provider = null;
        connectionProvider = null;
    }

    @Test
    void shouldStoreAndRetrievePayload() {
        provider = new ValkeySingleUseObjectProvider(session, connectionProvider, false);
        String key = "token-1";
        Map<String, String> notes = Map.of("foo", "bar");

        provider.put(key, 5, notes);

        assertEquals(notes, provider.get(key));
    }

    @Test
    void removeShouldBeSingleUse() {
        provider = new ValkeySingleUseObjectProvider(session, connectionProvider, false);
        String key = "token-2";
        provider.put(key, 5, Map.of("a", "b"));

        assertEquals(Map.of("a", "b"), provider.remove(key));
        assertNull(provider.remove(key));
    }

    @Test
    void replaceShouldRespectExistingTtl() {
        provider = new ValkeySingleUseObjectProvider(session, connectionProvider, false);
        String key = "token-3";
        provider.put(key, 10, Map.of("first", "value"));

        StatefulRedisConnection<String, String> connection = connectionProvider.getConnection();
        long ttlBefore = connection.sync().pttl(key);

        assertTrue(provider.replace(key, Map.of("first", "updated")));

        long ttlAfter = connection.sync().pttl(key);
        assertTrue(ttlAfter <= ttlBefore);
        assertTrue(ttlAfter > 0);
        assertEquals(Map.of("first", "updated"), provider.get(key));
    }

    @Test
    void putIfAbsentShouldInsertOnlyOnce() {
        provider = new ValkeySingleUseObjectProvider(session, connectionProvider, false);
        String key = "token-4";

        assertTrue(provider.putIfAbsent(key, 5));
        assertFalse(provider.putIfAbsent(key, 5));
    }

    @Test
    void containsShouldReflectStoredKeys() {
        provider = new ValkeySingleUseObjectProvider(session, connectionProvider, false);
        String key = "token-5";
        provider.put(key, 5, Map.of());

        assertTrue(provider.contains(key));
        provider.remove(key);
        assertFalse(provider.contains(key));
    }

    @Test
    void revokedTokenShouldPersistToDatabase() {
        RevokedTokenPersisterProvider revokedTokenPersister = mock(RevokedTokenPersisterProvider.class);
        when(session.getProvider(eq(RevokedTokenPersisterProvider.class))).thenReturn(revokedTokenPersister);

        provider = new ValkeySingleUseObjectProvider(session, connectionProvider, true);
        String key = "revoked" + SingleUseObjectProvider.REVOKED_KEY;

        provider.put(key, 30, Map.of());

        verify(revokedTokenPersister).revokeToken(eq("revoked"), anyLong());
        assertTrue(provider.contains(key));
    }

    @Test
    void revokedTokenShouldRejectNotes() {
        provider = new ValkeySingleUseObjectProvider(session, connectionProvider, true);
        String key = "another" + SingleUseObjectProvider.REVOKED_KEY;

        assertThrows(ModelException.class, () -> provider.put(key, 30, Map.of("extra", "note")));
    }

    @Test
    void revokedTokenShouldDisallowRetrieval() {
        RevokedTokenPersisterProvider revokedTokenPersister = mock(RevokedTokenPersisterProvider.class);
        when(session.getProvider(eq(RevokedTokenPersisterProvider.class))).thenReturn(revokedTokenPersister);

        provider = new ValkeySingleUseObjectProvider(session, connectionProvider, true);
        String key = "restricted" + SingleUseObjectProvider.REVOKED_KEY;
        provider.put(key, 30, Map.of());

        assertThrows(ModelException.class, () -> provider.get(key));
        assertThrows(ModelException.class, () -> provider.remove(key));
    }
}
