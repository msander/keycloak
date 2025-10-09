package org.keycloak.valkey.connection;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.keycloak.valkey.ValkeyConnectionProvider;
import org.keycloak.valkey.testing.EmbeddedValkeyServer;
import org.keycloak.valkey.testing.MapBackedConfigScope;
import org.keycloak.valkey.testing.PortAllocator;

import io.lettuce.core.api.StatefulRedisConnection;

class LettuceValkeyConnectionProviderFactoryTest {

    private static EmbeddedValkeyServer server;
    private static LettuceValkeyConnectionProviderFactory factory;

    private ValkeyConnectionProvider provider;

    @BeforeAll
    static void startServer() {
        server = EmbeddedValkeyServer.builder()
                .startupTimeout(Duration.ofSeconds(5))
                .start();
        factory = new LettuceValkeyConnectionProviderFactory();
    }

    @AfterEach
    void tearDownProvider() {
        if (provider != null) {
            provider.close();
        }
        provider = null;
    }

    @AfterAll
    static void shutdown() {
        if (factory != null) {
            factory.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    void shouldConnectAndExecuteCommands() {
        Map<String, String> configValues = new HashMap<>();
        configValues.put("uri", "redis://" + server.getHost() + ":" + server.getPort());
        configValues.put("command-timeout", "PT2S");
        MapBackedConfigScope scope = MapBackedConfigScope.from(configValues);

        factory.init(scope);
        provider = factory.create(null);

        StatefulRedisConnection<String, String> connection = provider.getConnection();
        connection.sync().set("key", "value");
        assertEquals("value", connection.sync().get("key"));
    }

    @Test
    void shouldReuseRedisClientAcrossSessions() {
        Map<String, String> configValues = new HashMap<>();
        configValues.put("uri", "redis://" + server.getHost() + ":" + server.getPort());
        factory.init(MapBackedConfigScope.from(configValues));

        ValkeyConnectionProvider first = factory.create(null);
        ValkeyConnectionProvider second = factory.create(null);
        try {
            assertSame(first.getRedisClient(), second.getRedisClient());
        } finally {
            first.close();
            second.close();
        }
    }

    @Test
    void binaryConnectionShouldSupportRawPayloads() {
        Map<String, String> configValues = new HashMap<>();
        configValues.put("uri", "redis://" + server.getHost() + ":" + server.getPort());
        factory.init(MapBackedConfigScope.from(configValues));
        provider = factory.create(null);

        byte[] payload = new byte[] {1, 2, 3, 4};
        provider.getBinaryConnection().sync().set("binary-key", payload);
        assertArrayEquals(payload, provider.getBinaryConnection().sync().get("binary-key"));
    }

    @Test
    void shouldExposeOperationalInfoWithHealth() {
        Map<String, String> configValues = new HashMap<>();
        configValues.put("uri", "redis://" + server.getHost() + ":" + server.getPort());
        configValues.put("client-name", "factory-test");
        factory.init(MapBackedConfigScope.from(configValues));

        Map<String, String> info = factory.getOperationalInfo();

        assertEquals("redis://" + server.getHost() + ":" + server.getPort(), info.get("config.uri"));
        assertEquals("factory-test", info.get("config.clientName"));
        assertEquals("false", info.get("config.ssl"));

        assertEquals("UP", info.get("health.status"));
        assertTrue(Long.parseLong(info.get("health.latencyMs")) >= 0L);
        assertNotNull(info.get("health.timestamp"));
        assertFalse(info.containsKey("health.error"));
    }

    @Test
    void shouldReportDownHealthWhenServerUnavailable() {
        int port = PortAllocator.findFreePort();
        LettuceValkeyConnectionProviderFactory failingFactory = new LettuceValkeyConnectionProviderFactory();
        Map<String, String> configValues = new HashMap<>();
        configValues.put("uri", "redis://127.0.0.1:" + port);
        configValues.put("connect-timeout", "PT0.2S");
        configValues.put("command-timeout", "PT0.2S");
        failingFactory.init(MapBackedConfigScope.from(configValues));

        try {
            Map<String, String> info = failingFactory.getOperationalInfo();
            assertEquals("DOWN", info.get("health.status"));
            assertNotNull(info.get("health.error"));
        } finally {
            failingFactory.close();
        }
    }
}
