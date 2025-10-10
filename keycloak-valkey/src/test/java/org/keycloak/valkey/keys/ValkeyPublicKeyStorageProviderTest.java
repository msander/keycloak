package org.keycloak.valkey.keys;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.keycloak.cluster.ClusterEvent;
import org.keycloak.cluster.ClusterListener;
import org.keycloak.cluster.ClusterProvider;
import org.keycloak.cluster.ExecutionResult;
import org.keycloak.common.util.Time;
import org.keycloak.crypto.PublicKeysWrapper;
import org.keycloak.keys.PublicKeyLoader;
import org.keycloak.keys.PublicKeyStorageProvider;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.models.KeycloakTransaction;
import org.keycloak.models.KeycloakTransactionManager;
import org.keycloak.valkey.testing.MapBackedConfigScope;
import org.mockito.Mockito;

class ValkeyPublicKeyStorageProviderTest {

    private final AtomicInteger loadCounter = new AtomicInteger();

    private ValkeyPublicKeyStorageProviderFactory factory;
    private KeycloakSession session;
    private StubClusterProvider cluster;
    private StubTransactionManager txManager;

    @BeforeEach
    void setUp() {
        Time.setOffset(0);
        ValkeyPublicKeyStorageProviderFactory.cache().clear();
        loadCounter.set(0);

        factory = new ValkeyPublicKeyStorageProviderFactory();
        factory.init(MapBackedConfigScope.from(Map.of("minTimeBetweenRequests", "10", "maxCacheTime", "600")));

        cluster = new StubClusterProvider();
        txManager = new StubTransactionManager();
        session = Mockito.mock(KeycloakSession.class);
        Mockito.when(session.getProvider(Mockito.eq(ClusterProvider.class))).thenReturn(cluster);
        Mockito.when(session.getTransactionManager()).thenReturn(txManager);
        KeycloakSessionFactory sessionFactory = Mockito.mock(KeycloakSessionFactory.class);
        Mockito.when(session.getKeycloakSessionFactory()).thenReturn(sessionFactory);
        Mockito.when(session.getProvider(Mockito.eq(PublicKeyStorageProvider.class), Mockito.eq(factory.getId())))
                .thenAnswer(invocation -> factory.create(session));
    }

    @AfterEach
    void tearDown() {
        Time.setOffset(0);
        ValkeyPublicKeyStorageProviderFactory.cache().clear();
    }

    @Test
    void shouldLoadKeysOnceUnderConcurrency() throws Exception {
        PublicKeyLoader loader = () -> {
            loadCounter.incrementAndGet();
            return PublicKeysWrapper.EMPTY;
        };

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Thread thread = new Thread(() -> {
                ValkeyPublicKeyStorageProvider provider = factory.create(session);
                provider.getPublicKey("model", "kid", null, loader);
            });
            threads.add(thread);
        }
        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join();
        }
        assertEquals(1, loadCounter.get());

        Time.setOffset(20);
        threads.clear();
        for (int i = 0; i < 5; i++) {
            Thread thread = new Thread(() -> {
                ValkeyPublicKeyStorageProvider provider = factory.create(session);
                provider.getPublicKey("model", "kid", null, loader);
            });
            threads.add(thread);
        }
        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join();
        }
        assertEquals(2, loadCounter.get());
    }

    @Test
    void shouldPropagateInvalidationAfterTransaction() throws Exception {
        ValkeyPublicKeyStorageProvider provider = factory.create(session);
        PublicKeyLoader loader = () -> PublicKeysWrapper.EMPTY;

        provider.getPublicKey("realm-client", "kid", null, loader);
        assertFalse(ValkeyPublicKeyStorageProviderFactory.cache().isEmpty());

        String cacheKey = "realm-client";
        provider.addInvalidation(cacheKey);
        assertTrue(cluster.notifications.isEmpty());

        txManager.complete();
        assertEquals(1, cluster.notifications.size());
        StubClusterProvider.Notification notification = cluster.notifications.get(0);
        assertEquals(ValkeyPublicKeyStorageProviderFactory.PUBLIC_KEY_STORAGE_INVALIDATION_EVENT,
                notification.taskKey());
        assertTrue(ValkeyPublicKeyStorageProviderFactory.cache().isEmpty());

        // Simulate remote event dispatch
        cluster.fire(ValkeyPublicKeyStorageProviderFactory.PUBLIC_KEY_STORAGE_INVALIDATION_EVENT,
                java.util.List.of(ValkeyPublicKeyInvalidationEvent.create("other")));
        assertTrue(ValkeyPublicKeyStorageProviderFactory.cache().isEmpty());
    }

    private static final class StubTransactionManager implements KeycloakTransactionManager {

        private final List<KeycloakTransaction> afterCompletion = new ArrayList<>();

        @Override
        public void enlist(KeycloakTransaction transaction) {
        }

        @Override
        public void enlistAfterCompletion(KeycloakTransaction transaction) {
            afterCompletion.add(transaction);
        }

        @Override
        public void enlistPrepare(KeycloakTransaction transaction) {
        }

        void complete() {
            for (KeycloakTransaction tx : afterCompletion) {
                tx.commit();
            }
            afterCompletion.clear();
        }

        @Override
        public JTAPolicy getJTAPolicy() {
            return JTAPolicy.NOT_SUPPORTED;
        }

        @Override
        public void setJTAPolicy(JTAPolicy policy) {
        }

        @Override
        public void begin() {
        }

        @Override
        public void commit() {
        }

        @Override
        public void rollback() {
        }

        @Override
        public void setRollbackOnly() {
        }

        @Override
        public boolean getRollbackOnly() {
            return false;
        }

        @Override
        public boolean isActive() {
            return true;
        }
    }

    private static final class StubClusterProvider implements ClusterProvider {
        private final Map<String, List<ClusterListener>> listeners = new ConcurrentHashMap<>();
        private final Map<String, CompletableFuture<Boolean>> locks = new ConcurrentHashMap<>();
        final List<Notification> notifications = new ArrayList<>();

        @Override
        public int getClusterStartupTime() {
            return 0;
        }

        @Override
        public <T> ExecutionResult<T> executeIfNotExecuted(String taskKey, int taskTimeoutInSeconds,
                java.util.concurrent.Callable<T> task) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Future<Boolean> executeIfNotExecutedAsync(String taskKey, int taskTimeoutInSeconds, java.util.concurrent.Callable task) {
            return CompletableFuture.completedFuture(Boolean.FALSE);
        }

        @Override
        public void registerListener(String taskKey, ClusterListener task) {
            listeners.computeIfAbsent(taskKey, key -> new ArrayList<>()).add(task);
        }

        @Override
        public void notify(String taskKey, ClusterEvent event, boolean ignoreSender, DCNotify dcNotify) {
            notify(taskKey, List.of(event), ignoreSender, dcNotify);
        }

        @Override
        public void notify(String taskKey, ClusterEvent event, boolean ignoreSender) {
            notify(taskKey, List.of(event), ignoreSender, DCNotify.ALL_DCS);
        }

        @Override
        public void notify(String taskKey, java.util.Collection<? extends ClusterEvent> events, boolean ignoreSender,
                DCNotify dcNotify) {
            notifications.add(new Notification(taskKey, new ArrayList<>(events)));
            fire(taskKey, events);
        }

        @Override
        public void notify(String taskKey, java.util.Collection<? extends ClusterEvent> events, boolean ignoreSender) {
            notify(taskKey, events, ignoreSender, DCNotify.ALL_DCS);
        }

        void fire(String taskKey, java.util.Collection<? extends ClusterEvent> events) {
            List<ClusterListener> tasks = listeners.get(taskKey);
            if (tasks == null) {
                return;
            }
            for (ClusterListener listener : tasks) {
                for (ClusterEvent event : events) {
                    listener.eventReceived(event);
                }
            }
        }

        record Notification(String taskKey, List<ClusterEvent> events) {
        }

        @Override
        public void close() {
        }
    }
}
