package org.keycloak.valkey.dblock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.dblock.DBLockProvider;
import org.keycloak.models.dblock.DBLockProvider.Namespace;
import org.keycloak.valkey.ValkeyConnectionProvider;
import org.keycloak.valkey.connection.LettuceValkeyConnectionProviderFactory;
import org.keycloak.valkey.testing.EmbeddedValkeyServer;
import org.keycloak.valkey.testing.MapBackedConfigScope;

class ValkeyDBLockProviderFactoryTest {

    private static EmbeddedValkeyServer server;
    private static LettuceValkeyConnectionProviderFactory connectionFactory;
    private static Map<String, String> baseConfig;

    private final List<ValkeyConnectionProvider> allocations = new ArrayList<>();
    private ValkeyDBLockProviderFactory factory;

    @BeforeAll
    static void startInfrastructure() {
        server = EmbeddedValkeyServer.builder()
                .startupTimeout(Duration.ofSeconds(5))
                .start();

        connectionFactory = new LettuceValkeyConnectionProviderFactory();
        Map<String, String> connectionConfig = new HashMap<>();
        connectionConfig.put("uri", "redis://" + server.getHost() + ':' + server.getPort());
        connectionFactory.init(MapBackedConfigScope.from(connectionConfig));

        baseConfig = new HashMap<>();
        baseConfig.put("namespace", "keycloak:test:dblock");
        baseConfig.put("lock-recheck-time", "25");
        baseConfig.put("lock-wait-timeout", "200");
        baseConfig.put("lock-lease", "1000");
    }

    @AfterAll
    static void shutdownInfrastructure() {
        if (connectionFactory != null) {
            connectionFactory.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @BeforeEach
    void setUpFactory() {
        factory = new ValkeyDBLockProviderFactory();
        factory.init(MapBackedConfigScope.from(baseConfig));
    }

    @AfterEach
    void cleanup() {
        for (ValkeyConnectionProvider provider : allocations) {
            provider.close();
        }
        allocations.clear();
        if (factory != null) {
            factory.close();
        }
    }

    @Test
    void shouldCoordinateLockAcrossSessions() {
        DBLockProvider lock1 = createProvider();
        DBLockProvider lock2 = createProvider();

        lock1.waitForLock(Namespace.DATABASE);
        assertEquals(Namespace.DATABASE, lock1.getCurrentLock());

        RuntimeException blocked = assertThrows(RuntimeException.class, () -> lock2.waitForLock(Namespace.DATABASE));
        assertTrue(blocked.getMessage().contains("Failed to acquire DB lock"));

        lock1.releaseLock();
        lock2.waitForLock(Namespace.DATABASE);
        assertEquals(Namespace.DATABASE, lock2.getCurrentLock());
        lock2.releaseLock();
    }

    @Test
    void shouldPreventNestedLocksWithinSession() {
        DBLockProvider lock = createProvider();
        lock.waitForLock(Namespace.DATABASE);
        RuntimeException nested = assertThrows(RuntimeException.class, () -> lock.waitForLock(Namespace.KEYCLOAK_BOOT));
        assertTrue(nested.getMessage().contains("Trying to get a lock"));
        lock.releaseLock();
    }

    @Test
    void shouldDestroyLockInfo() {
        String key = lockKey("keycloak:test:dblock", Namespace.DATABASE);
        withRawRedis(commands -> commands.set(key, "stale"));

        DBLockProvider lock = createProvider();
        lock.destroyLockInfo();

        String remaining = withRawRedis(commands -> commands.get(key));
        assertNull(remaining);
    }

    @Test
    void shouldForceUnlockOnStartupWhenConfigured() {
        String key = lockKey("keycloak:force:dblock", Namespace.KEYCLOAK_BOOT);
        withRawRedis(commands -> commands.set(key, "blocked"));

        Map<String, String> config = new HashMap<>(baseConfig);
        config.put("namespace", "keycloak:force:dblock");
        config.put("force-unlock-on-startup", "true");

        ValkeyDBLockProviderFactory unlockingFactory = new ValkeyDBLockProviderFactory();
        unlockingFactory.init(MapBackedConfigScope.from(config));

        DBLockProvider lock = createProvider(unlockingFactory);
        lock.waitForLock(Namespace.KEYCLOAK_BOOT);
        lock.releaseLock();

        String remaining = withRawRedis(commands -> commands.get(key));
        assertNull(remaining);
        unlockingFactory.close();
    }

    @Test
    void shouldReportForcedUnlockSupport() {
        DBLockProvider lock = createProvider();
        assertTrue(lock.supportsForcedUnlock());
    }

    private DBLockProvider createProvider() {
        return createProvider(factory);
    }

    private DBLockProvider createProvider(ValkeyDBLockProviderFactory providerFactory) {
        KeycloakSession session = mock(KeycloakSession.class);
        ValkeyConnectionProvider connectionProvider = connectionFactory.create(session);
        allocations.add(connectionProvider);
        when(session.getProvider(ValkeyConnectionProvider.class)).thenReturn(connectionProvider);
        return providerFactory.create(session);
    }

    private <T> T withRawRedis(java.util.function.Function<io.lettuce.core.api.sync.RedisCommands<String, String>, T> fn) {
        ValkeyConnectionProvider provider = connectionFactory.create(mock(KeycloakSession.class));
        try {
            return fn.apply(provider.getConnection().sync());
        } finally {
            provider.close();
        }
    }

    private static String lockKey(String namespace, Namespace namespaceId) {
        return namespace + ':' + namespaceId.name().toLowerCase(Locale.ROOT).replace('_', '-');
    }
}
