package org.keycloak.valkey.keys;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.keycloak.cluster.ClusterEvent;
import org.keycloak.cluster.ClusterListener;
import org.keycloak.cluster.ClusterProvider;
import org.keycloak.cluster.ExecutionResult;
import org.keycloak.common.util.Time;
import org.keycloak.crypto.PublicKeysWrapper;
import org.keycloak.keys.PublicKeyStorageProvider;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakTransaction;
import org.keycloak.models.KeycloakTransactionManager;
import org.keycloak.valkey.testing.MapBackedConfigScope;
import org.mockito.Mockito;

class ValkeyCachePublicKeyProviderFactoryTest {

    private KeycloakSession session;
    private ValkeyPublicKeyStorageProviderFactory storageFactory;
    private ValkeyCachePublicKeyProviderFactory cacheFactory;
    private StubClusterProvider cluster;

    @BeforeEach
    void setUp() {
        Time.setOffset(0);
        ValkeyPublicKeyStorageProviderFactory.cache().clear();
        storageFactory = new ValkeyPublicKeyStorageProviderFactory();
        storageFactory.init(MapBackedConfigScope.from(Map.of()));
        cacheFactory = new ValkeyCachePublicKeyProviderFactory();

        cluster = new StubClusterProvider();
        session = Mockito.mock(KeycloakSession.class);
        Mockito.when(session.getProvider(Mockito.eq(ClusterProvider.class))).thenReturn(cluster);
        Mockito.when(session.getTransactionManager()).thenReturn(new StubTransactionManager());
        Mockito.when(session.getProvider(Mockito.eq(PublicKeyStorageProvider.class), Mockito.eq(storageFactory.getId())))
                .thenAnswer(invocation -> storageFactory.create(session));
    }

    @AfterEach
    void tearDown() {
        ValkeyPublicKeyStorageProviderFactory.cache().clear();
    }

    @Test
    void shouldClearCacheAndBroadcastEvent() {
        ValkeyPublicKeyStorageProviderFactory.cache()
                .put("entry", new ValkeyPublicKeysEntry(Time.currentTime(), PublicKeysWrapper.EMPTY));
        ValkeyCachePublicKeyProvider provider = (ValkeyCachePublicKeyProvider) cacheFactory.create(session);

        provider.clearCache();
        assertTrue(ValkeyPublicKeyStorageProviderFactory.cache().isEmpty());
        assertEquals(1, cluster.notifications.size());
        assertEquals(ValkeyPublicKeyStorageProviderFactory.KEYS_CLEAR_CACHE_EVENT,
                cluster.notifications.get(0).taskKey());

        cluster.fire(ValkeyPublicKeyStorageProviderFactory.KEYS_CLEAR_CACHE_EVENT,
                java.util.List.of(ValkeyClearCacheEvent.getInstance()));
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
            for (KeycloakTransaction tx : afterCompletion) {
                tx.commit();
            }
            afterCompletion.clear();
        }

        @Override
        public void rollback() {
            for (KeycloakTransaction tx : afterCompletion) {
                tx.rollback();
            }
            afterCompletion.clear();
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
        public java.util.concurrent.Future<Boolean> executeIfNotExecutedAsync(String taskKey, int taskTimeoutInSeconds,
                java.util.concurrent.Callable task) {
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
