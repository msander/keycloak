package org.keycloak.valkey.loginfailure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
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
import org.keycloak.models.RealmModel;
import org.keycloak.models.UserLoginFailureModel;
import org.keycloak.valkey.ValkeyConnectionProvider;
import org.keycloak.valkey.connection.LettuceValkeyConnectionProviderFactory;
import org.keycloak.valkey.testing.EmbeddedValkeyServer;
import org.keycloak.valkey.testing.MapBackedConfigScope;

import io.lettuce.core.api.StatefulRedisConnection;

class ValkeyUserLoginFailureProviderTest {

    private static final String NAMESPACE = "test:login-failure";

    private static EmbeddedValkeyServer server;
    private static LettuceValkeyConnectionProviderFactory connectionFactory;

    private ValkeyConnectionProvider connectionProvider;
    private KeycloakSession session;
    private RealmModel realm;
    private ValkeyUserLoginFailureProviderFactory factory;
    private ValkeyUserLoginFailureProvider provider;

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
        realm = mockRealm();

        factory = new ValkeyUserLoginFailureProviderFactory();
        Map<String, String> config = new HashMap<>();
        config.put("namespace", NAMESPACE);
        config.put("min-lifespan", "PT10M");
        factory.init(MapBackedConfigScope.from(config));
        provider = factory.create(session);
    }

    @AfterEach
    void tearDown() {
        if (provider != null) {
            provider.close();
        }
        if (connectionProvider != null) {
            connectionProvider.close();
        }
    }

    @Test
    void shouldCreateAndRetrieveLoginFailure() {
        UserLoginFailureModel model = provider.addUserLoginFailure(realm, "user-1");
        assertNotNull(model);
        assertEquals("user-1", model.getUserId());
        assertEquals(0, model.getNumFailures());

        model.incrementFailures();
        model.incrementTemporaryLockouts();
        model.setFailedLoginNotBefore(1000);
        model.setLastFailure(5_000L);
        model.setLastIPFailure("127.0.0.1");

        UserLoginFailureModel reloaded = provider.getUserLoginFailure(realm, "user-1");
        assertNotNull(reloaded);
        assertEquals(1, reloaded.getNumFailures());
        assertEquals(1, reloaded.getNumTemporaryLockouts());
        assertEquals(1000, reloaded.getFailedLoginNotBefore());
        assertEquals(5_000L, reloaded.getLastFailure());
        assertEquals("127.0.0.1", reloaded.getLastIPFailure());

        long ttl = readTtl("user-1");
        assertTrue(ttl > 0, "TTL should be applied to login failure entry");
    }

    @Test
    void shouldRespectMaxWhenSettingTimestamps() {
        UserLoginFailureModel model = provider.addUserLoginFailure(realm, "user-2");
        model.setFailedLoginNotBefore(500);
        model.setFailedLoginNotBefore(100); // should be ignored

        model.setLastFailure(10_000L);
        model.setLastFailure(1_000L); // should be ignored

        assertEquals(500, model.getFailedLoginNotBefore());
        assertEquals(10_000L, model.getLastFailure());
    }

    @Test
    void shouldClearFailuresAndRemoveIp() {
        UserLoginFailureModel model = provider.addUserLoginFailure(realm, "user-3");
        model.incrementFailures();
        model.incrementTemporaryLockouts();
        model.setLastFailure(1000L);
        model.setFailedLoginNotBefore(400);
        model.setLastIPFailure("192.168.0.5");

        model.clearFailures();

        assertEquals(0, model.getNumFailures());
        assertEquals(0, model.getNumTemporaryLockouts());
        assertEquals(0, model.getFailedLoginNotBefore());
        assertEquals(0L, model.getLastFailure());
        assertNull(model.getLastIPFailure());
    }

    @Test
    void removeAllShouldOnlyDeleteRealmSpecificFailures() {
        RealmModel otherRealm = mockRealm("other-realm");
        provider.addUserLoginFailure(realm, "user-4");
        provider.addUserLoginFailure(otherRealm, "user-4");

        provider.removeAllUserLoginFailures(realm);

        assertNull(provider.getUserLoginFailure(realm, "user-4"));
        assertNotNull(provider.getUserLoginFailure(otherRealm, "user-4"));
    }

    private long readTtl(String userId) {
        StatefulRedisConnection<String, String> connection = connectionProvider.getConnection();
        return connection.sync().pttl(NAMESPACE + ':' + realm.getId() + ':' + userId);
    }

    private RealmModel mockRealm() {
        return mockRealm("realm-1");
    }

    private RealmModel mockRealm(String realmId) {
        RealmModel mock = mock(RealmModel.class);
        when(mock.getId()).thenReturn(realmId);
        when(mock.getMaxDeltaTimeSeconds()).thenReturn(3600);
        when(mock.getMaxFailureWaitSeconds()).thenReturn(1800);
        when(mock.getMinimumQuickLoginWaitSeconds()).thenReturn(60);
        when(mock.getQuickLoginCheckMilliSeconds()).thenReturn(5000L);
        return mock;
    }
}
