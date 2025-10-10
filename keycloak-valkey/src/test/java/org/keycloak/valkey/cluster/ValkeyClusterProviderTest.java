package org.keycloak.valkey.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.keycloak.cluster.ClusterEvent;
import org.keycloak.cluster.ClusterListener;
import org.keycloak.cluster.ClusterProvider;
import org.keycloak.cluster.ExecutionResult;
import org.keycloak.executors.ExecutorsProvider;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.valkey.ValkeyConnectionProvider;
import org.keycloak.valkey.connection.LettuceValkeyConnectionProviderFactory;
import org.keycloak.valkey.testing.EmbeddedValkeyServer;
import org.keycloak.valkey.testing.MapBackedConfigScope;
import org.mockito.Answers;

class ValkeyClusterProviderTest {

    private static EmbeddedValkeyServer server;
    private static LettuceValkeyConnectionProviderFactory connectionFactory;
    private static ValkeyClusterProviderFactory clusterFactory;
    private static TestExecutorsProvider executorsProvider;
    private static Map<String, String> clusterConfig;

    private ValkeyConnectionProvider provider1;
    private ValkeyConnectionProvider provider2;
    private ClusterProvider cluster1;
    private ClusterProvider cluster2;

    @BeforeAll
    static void startInfrastructure() {
        server = EmbeddedValkeyServer.builder()
                .startupTimeout(Duration.ofSeconds(5))
                .start();
        connectionFactory = new LettuceValkeyConnectionProviderFactory();
        Map<String, String> connectionConfig = new HashMap<>();
        connectionConfig.put("uri", "redis://" + server.getHost() + ':' + server.getPort());
        connectionFactory.init(MapBackedConfigScope.from(connectionConfig));

        clusterFactory = new ValkeyClusterProviderFactory();
        clusterConfig = new HashMap<>();
        clusterConfig.put("namespace", "keycloak:test");
        clusterFactory.init(MapBackedConfigScope.from(clusterConfig));

        executorsProvider = new TestExecutorsProvider();
    }

    @AfterEach
    void tearDownProviders() {
        if (cluster1 != null) {
            cluster1.close();
        }
        if (cluster2 != null) {
            cluster2.close();
        }
        if (provider1 != null) {
            provider1.close();
        }
        if (provider2 != null) {
            provider2.close();
        }
        cluster1 = null;
        cluster2 = null;
        provider1 = null;
        provider2 = null;
    }

    @AfterAll
    static void shutdownInfrastructure() {
        if (connectionFactory != null) {
            connectionFactory.close();
        }
        if (executorsProvider != null) {
            executorsProvider.close();
        }
        if (server != null) {
            server.close();
        }
        if (clusterFactory != null) {
            clusterFactory.close();
        }
    }

    @Test
    void shouldExecuteTaskOnceAcrossProviders() throws Exception {
        setUpProviders();

        AtomicInteger counter = new AtomicInteger();
        java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            Future<ExecutionResult<String>> primary = executor.submit(() -> cluster1.executeIfNotExecuted("job", 5, () -> {
                entered.countDown();
                release.await(2, TimeUnit.SECONDS);
                counter.incrementAndGet();
                return "done";
            }));

            assertTrue(entered.await(2, TimeUnit.SECONDS));
            Future<ExecutionResult<String>> secondary = executor.submit(() -> cluster2.executeIfNotExecuted("job", 5, () -> {
                counter.incrementAndGet();
                return "other";
            }));

            ExecutionResult<String> second = secondary.get(5, TimeUnit.SECONDS);
            assertFalse(second.isExecuted());

            release.countDown();
            ExecutionResult<String> first = primary.get(5, TimeUnit.SECONDS);
            assertTrue(first.isExecuted());
            assertEquals("done", first.getResult());
        } finally {
            executor.shutdownNow();
        }
        assertEquals(1, counter.get());
        assertEquals(cluster1.getClusterStartupTime(), cluster2.getClusterStartupTime());
    }

    @Test
    void shouldCoordinateAsyncExecution() throws Exception {
        setUpProviders();

        AtomicInteger counter = new AtomicInteger();
        Callable<Void> task = () -> {
            counter.incrementAndGet();
            return null;
        };

        java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);

        Future<Boolean> primary = cluster1.executeIfNotExecutedAsync("async", 5, (Callable<Void>) () -> {
            entered.countDown();
            release.await(2, TimeUnit.SECONDS);
            counter.incrementAndGet();
            return null;
        });

        assertTrue(entered.await(5, TimeUnit.SECONDS));
        Future<Boolean> secondary = cluster2.executeIfNotExecutedAsync("async", 5, task);

        assertFalse(secondary.get(5, TimeUnit.SECONDS));
        release.countDown();
        assertTrue(primary.get(5, TimeUnit.SECONDS));
        assertEquals(1, counter.get());
    }

    @Test
    void shouldCompleteAsyncWaitersViaEvents() throws Exception {
        clusterConfig.put("completion-poll-interval", "PT5S");
        try {
            setUpProviders();

            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);

            Future<Boolean> primary = cluster1.executeIfNotExecutedAsync("async-events", 5, (Callable<Void>) () -> {
                entered.countDown();
                release.await(2, TimeUnit.SECONDS);
                return null;
            });

            assertTrue(entered.await(5, TimeUnit.SECONDS));
            long start = System.nanoTime();
            Future<Boolean> secondary = cluster2.executeIfNotExecutedAsync("async-events", 5,
                    (Callable<Void>) () -> null);

            release.countDown();
            assertFalse(secondary.get(5, TimeUnit.SECONDS));
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertTrue(elapsed < 3000, "Event-driven completion should finish before poll interval elapsed");

            assertTrue(primary.get(5, TimeUnit.SECONDS));
        } finally {
            clusterConfig.remove("completion-poll-interval");
        }
    }

    @Test
    void shouldDispatchEventsToLocalListeners() throws Exception {
        setUpProviders();

        AtomicInteger events = new AtomicInteger();
        cluster1.registerListener("channel", new ClusterListener() {
            @Override
            public void eventReceived(ClusterEvent event) {
                events.incrementAndGet();
            }
        });

        ClusterEvent event = new TestClusterEvent("local-1", "notify");

        cluster1.notify("channel", event, false);
        cluster1.notify("channel", java.util.List.of(event, event), false);

        assertEquals(3, events.get());
    }

    @Test
    void shouldPropagateEventsAcrossFactories() throws Exception {
        setUpProviders();

        AtomicInteger events = new AtomicInteger();
        CountDownLatch received = new CountDownLatch(1);
        cluster1.registerListener("remote", new ClusterListener() {
            @Override
            public void eventReceived(ClusterEvent event) {
                events.incrementAndGet();
                received.countDown();
            }
        });

        ClusterEvent event = new TestClusterEvent("remote-1", "notify");

        ValkeyClusterProviderFactory remoteFactory = new ValkeyClusterProviderFactory();
        remoteFactory.init(MapBackedConfigScope.from(clusterConfig));
        ValkeyConnectionProvider remoteProvider = connectionFactory.create(null);
        ClusterProvider remoteCluster = remoteFactory.create(mockSession(remoteProvider, System.currentTimeMillis() + 5_000));
        try {
            remoteCluster.notify("remote", event, true, ClusterProvider.DCNotify.ALL_DCS);
            assertTrue(received.await(5, TimeUnit.SECONDS));
        } finally {
            remoteCluster.close();
            remoteProvider.close();
            remoteFactory.close();
        }

        assertEquals(1, events.get());
    }

    @Test
    void shouldRespectDataCenterFiltering() throws Exception {
        clusterConfig.put("site", "primary");
        try {
            setUpProviders();

            CountDownLatch received = new CountDownLatch(1);
            cluster1.registerListener("dc-filter", new ClusterListener() {
                @Override
                public void eventReceived(ClusterEvent event) {
                    received.countDown();
                }
            });

            Map<String, String> remoteConfig = new HashMap<>(clusterConfig);
            remoteConfig.put("site", "secondary");
            ValkeyClusterProviderFactory remoteFactory = new ValkeyClusterProviderFactory();
            remoteFactory.init(MapBackedConfigScope.from(remoteConfig));
            ValkeyConnectionProvider remoteProvider = connectionFactory.create(null);
            ClusterProvider remoteCluster = remoteFactory
                    .create(mockSession(remoteProvider, System.currentTimeMillis() + 10_000));
            try {
                remoteCluster.notify("dc-filter", new TestClusterEvent("dc-local", "only"), true,
                        ClusterProvider.DCNotify.LOCAL_DC_ONLY);
                assertFalse(received.await(500, TimeUnit.MILLISECONDS));

                remoteCluster.notify("dc-filter", new TestClusterEvent("dc-remote", "others"), true,
                        ClusterProvider.DCNotify.ALL_BUT_LOCAL_DC);
                assertTrue(received.await(5, TimeUnit.SECONDS));
            } finally {
                remoteCluster.close();
                remoteProvider.close();
                remoteFactory.close();
            }
        } finally {
            clusterConfig.remove("site");
        }
    }

    private void setUpProviders() {
        resetClusterFactory();
        provider1 = connectionFactory.create(null);
        provider2 = connectionFactory.create(null);

        long now = System.currentTimeMillis();
        cluster1 = clusterFactory.create(mockSession(provider1, now));
        cluster2 = clusterFactory.create(mockSession(provider2, now + 1_000));
    }

    private void resetClusterFactory() {
        if (clusterFactory != null) {
            clusterFactory.close();
        }
        clusterFactory = new ValkeyClusterProviderFactory();
        clusterFactory.init(MapBackedConfigScope.from(clusterConfig));
    }

    private KeycloakSession mockSession(ValkeyConnectionProvider provider, long startupMillis) {
        KeycloakSession session = mock(KeycloakSession.class, Answers.RETURNS_DEEP_STUBS);
        KeycloakSessionFactory sessionFactory = mock(KeycloakSessionFactory.class);

        org.mockito.Mockito.doReturn(provider).when(session).getProvider(ValkeyConnectionProvider.class);
        org.mockito.Mockito.doReturn(executorsProvider).when(session).getProvider(ExecutorsProvider.class);
        when(session.getKeycloakSessionFactory()).thenReturn(sessionFactory);
        when(sessionFactory.getServerStartupTimestamp()).thenReturn(startupMillis);
        return session;
    }

    private static final class TestExecutorsProvider implements ExecutorsProvider {
        private final ExecutorService executor = Executors.newCachedThreadPool();

        @Override
        public ExecutorService getExecutor(String taskType) {
            return executor;
        }

        @Override
        public void close() {
            executor.shutdownNow();
        }
    }
}
