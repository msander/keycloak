package org.keycloak.valkey.cluster;

import java.time.Duration;
import java.util.Base64;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.logging.Logger;
import org.keycloak.cluster.ClusterEvent;
import org.keycloak.cluster.ClusterListener;
import org.keycloak.cluster.ClusterProvider;
import org.keycloak.cluster.ExecutionResult;
import org.keycloak.common.util.ConcurrentMultivaluedHashMap;
import org.keycloak.valkey.config.ValkeyClusterConfig;
import org.keycloak.valkey.metrics.ValkeyMetrics;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.RedisClient;
import io.micrometer.core.instrument.Timer;

/**
 * Redis/Valkey backed implementation of {@link ClusterProvider} using optimistic locking primitives.
 */
public class ValkeyClusterProvider implements ClusterProvider {

    private static final Logger logger = Logger.getLogger(ValkeyClusterProvider.class);
    private static final String RELEASE_LOCK_SCRIPT = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end";
    private static final String WORK_COMPLETED_TASK_KEY = "valkey-work-completed";

    private final String nodeId;
    private final int clusterStartupTime;
    private final StatefulRedisConnection<String, String> commandConnection;
    private final StatefulRedisPubSubConnection<String, String> pubSubConnection;
    private final ValkeyClusterConfig config;
    private final ExecutorService executor;
    private final ValkeyClusterEventCodec codec;
    private final ConcurrentMultivaluedHashMap<String, ClusterListener> listeners = new ConcurrentMultivaluedHashMap<>();
    private final ConcurrentMap<String, CompletableFuture<Boolean>> asyncInvocations = new ConcurrentHashMap<>();

    ValkeyClusterProvider(String nodeId, int clusterStartupTime, RedisClient redisClient,
            ValkeyClusterConfig config, ExecutorService executor) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.clusterStartupTime = clusterStartupTime;
        Objects.requireNonNull(redisClient, "redisClient");
        this.config = Objects.requireNonNull(config, "config");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.codec = new ValkeyClusterEventCodec();
        this.commandConnection = redisClient.connect();
        this.pubSubConnection = redisClient.connectPubSub();
        subscribeForEvents();
        registerListener(WORK_COMPLETED_TASK_KEY, event -> {
            if (event instanceof ValkeyWorkCompletionEvent completion) {
                CompletableFuture<Boolean> future = asyncInvocations.get(completion.lockKey());
                if (future != null && !future.isDone()) {
                    future.complete(completion.success());
                }
            }
        });
    }

    @Override
    public int getClusterStartupTime() {
        return clusterStartupTime;
    }

    @Override
    public void close() {
        // Provider lifecycle is managed by the factory; resources are released via {@link #shutdown()}.
    }

    void shutdown() {
        listeners.clear();
        asyncInvocations.clear();
        try {
            pubSubConnection.close();
        } catch (RuntimeException ex) {
            logger.debugf(ex, "Failed to close Valkey pub/sub connection");
        }
        try {
            commandConnection.close();
        } catch (RuntimeException ex) {
            logger.debugf(ex, "Failed to close Valkey command connection");
        }
    }

    @Override
    public <T> ExecutionResult<T> executeIfNotExecuted(String taskKey, int taskTimeoutInSeconds, Callable<T> task) {
        String lockKey = config.taskKey(taskKey);
        long ttlMillis = Math.max(1, taskTimeoutInSeconds) * 1000L;
        String token = tokenFor(lockKey);
        var sample = ValkeyMetrics.startTimer();
        String response = commandConnection.sync().set(lockKey, token, SetArgs.Builder.nx().px(ttlMillis));
        boolean acquired = "OK".equalsIgnoreCase(response);
        if (!acquired) {
            if (logger.isTraceEnabled()) {
                logger.tracef("Lock already held for %s", lockKey);
            }
            ValkeyMetrics.record("cluster", "execute.sync", sample, ValkeyMetrics.Outcome.TIMEOUT);
            return ExecutionResult.notExecuted();
        }

        try {
            T result = task.call();
            ValkeyMetrics.record("cluster", "execute.sync", sample, ValkeyMetrics.Outcome.SUCCESS);
            return ExecutionResult.executed(result);
        } catch (RuntimeException ex) {
            ValkeyMetrics.record("cluster", "execute.sync", sample, ValkeyMetrics.Outcome.ERROR);
            throw ex;
        } catch (Exception ex) {
            ValkeyMetrics.record("cluster", "execute.sync", sample, ValkeyMetrics.Outcome.ERROR);
            throw new RuntimeException("Task execution failed for key " + taskKey, ex);
        } finally {
            releaseLock(lockKey, token);
        }
    }

    @Override
    public Future<Boolean> executeIfNotExecutedAsync(String taskKey, int taskTimeoutInSeconds, Callable task) {
        String lockKey = config.taskKey(taskKey);
        AtomicBoolean created = new AtomicBoolean(false);
        CompletableFuture<Boolean> managedFuture = asyncInvocations.compute(lockKey, (key, existing) -> {
            if (existing != null && !existing.isDone()) {
                return existing;
            }
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            created.set(true);
            return future;
        });
        if (created.get()) {
            var sample = ValkeyMetrics.startTimer();
            executor.submit(() -> runAsync(taskKey, taskTimeoutInSeconds, task, lockKey, managedFuture, sample));
            return managedFuture;
        }
        ValkeyMetrics.count("cluster", "execute.async", ValkeyMetrics.Outcome.TIMEOUT);
        return CompletableFuture.completedFuture(false);
    }

    private void runAsync(String taskKey, int timeoutSeconds, Callable task, String lockKey,
            CompletableFuture<Boolean> future, Timer.Sample sample) {
        try {
            ExecutionResult<?> result = executeIfNotExecuted(taskKey, timeoutSeconds, task);
            if (result.isExecuted()) {
                ValkeyMetrics.record("cluster", "execute.async", sample, ValkeyMetrics.Outcome.SUCCESS);
                future.complete(true);
                publishWorkCompletion(lockKey, true);
            } else {
                ValkeyMetrics.record("cluster", "execute.async", sample, ValkeyMetrics.Outcome.TIMEOUT);
                Timer.Sample waitSample = ValkeyMetrics.startTimer();
                waitForCompletion(lockKey, timeoutSeconds, future);
                if (future.complete(false)) {
                    ValkeyMetrics.record("cluster", "execute.async.wait", waitSample,
                            ValkeyMetrics.Outcome.TIMEOUT);
                } else {
                    ValkeyMetrics.record("cluster", "execute.async.wait", waitSample,
                            ValkeyMetrics.Outcome.SUCCESS);
                }
            }
        } catch (Throwable ex) {
            ValkeyMetrics.record("cluster", "execute.async", sample, ValkeyMetrics.Outcome.ERROR);
            publishWorkCompletion(lockKey, false);
            future.completeExceptionally(ex);
        } finally {
            asyncInvocations.remove(lockKey, future);
        }
    }

    private void waitForCompletion(String lockKey, int timeoutSeconds, CompletableFuture<Boolean> future) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(Math.max(timeoutSeconds, 1));
        Duration interval = config.getCompletionPollInterval();
        long sleepMillis = interval.toMillis();
        while (System.nanoTime() < deadline) {
            if (future.isDone() || !isLockPresent(lockKey)) {
                return;
            }
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private boolean isLockPresent(String lockKey) {
        return commandConnection.sync().get(lockKey) != null;
    }

    private void releaseLock(String lockKey, String token) {
        try {
            commandConnection.sync().eval(RELEASE_LOCK_SCRIPT, ScriptOutputType.INTEGER, new String[] {lockKey}, token);
        } catch (RuntimeException ex) {
            logger.debugf(ex, "Failed to release lock %s", lockKey);
        }
    }

    @Override
    public void registerListener(String taskKey, ClusterListener task) {
        listeners.add(taskKey, task);
    }

    @Override
    public void notify(String taskKey, ClusterEvent event, boolean ignoreSender, DCNotify dcNotify) {
        notify(taskKey, java.util.List.of(event), ignoreSender, dcNotify);
    }

    @Override
    public void notify(String taskKey, Collection<? extends ClusterEvent> events, boolean ignoreSender, DCNotify dcNotify) {
        if (events == null || events.isEmpty()) {
            return;
        }
        DCNotify effectiveNotify = dcNotify != null ? dcNotify : DCNotify.ALL_DCS;
        if (!ignoreSender) {
            dispatchLocal(taskKey, events);
        }
        publish(taskKey, events, ignoreSender, effectiveNotify);
        ValkeyMetrics.count("cluster", "notify", ValkeyMetrics.Outcome.SUCCESS);
    }

    @Override
    public void notify(String taskKey, ClusterEvent event, boolean ignoreSender) {
        notify(taskKey, event, ignoreSender, DCNotify.ALL_DCS);
    }

    @Override
    public void notify(String taskKey, Collection<? extends ClusterEvent> events, boolean ignoreSender) {
        notify(taskKey, events, ignoreSender, DCNotify.ALL_DCS);
    }

    private void dispatchLocal(String taskKey, Collection<? extends ClusterEvent> events) {
        Collection<ClusterListener> tasks = listeners.getList(taskKey);
        if (tasks == null || tasks.isEmpty()) {
            return;
        }
        for (ClusterListener listener : tasks) {
            for (ClusterEvent event : events) {
                try {
                    event.accept(listener);
                } catch (RuntimeException ex) {
                    logger.warnf(ex, "Listener %s failed to process event %s", listener, event);
                }
            }
        }
    }

    private String tokenFor(String lockKey) {
        return nodeId + ':' + lockKey + ':' + System.nanoTime();
    }

    private void subscribeForEvents() {
        pubSubConnection.addListener(new RedisPubSubAdapter<>() {
            @Override
            public void message(String channel, String message) {
                if (!Objects.equals(channel, config.getChannel())) {
                    return;
                }
                handleIncomingEvent(message);
            }
        });
        try {
            pubSubConnection.async().subscribe(config.getChannel()).toCompletableFuture().get(5, TimeUnit.SECONDS);
        } catch (Exception ex) {
            throw new IllegalStateException("Unable to subscribe to Valkey cluster channel", ex);
        }
    }

    private void handleIncomingEvent(String message) {
        try {
            byte[] payload = Base64.getDecoder().decode(message);
            ValkeyClusterEventCodec.DecodedMessage decoded = codec.decode(payload);
            if (!decoded.shouldDeliver(nodeId, config.getSiteName())) {
                return;
            }
            dispatchLocal(decoded.eventKey(), decoded.events());
        } catch (IllegalArgumentException | IllegalStateException ex) {
            logger.warnf(ex, "Failed to decode Valkey cluster event payload");
        }
    }

    private void publish(String taskKey, Collection<? extends ClusterEvent> events, boolean ignoreSender,
            DCNotify dcNotify) {
        DCNotify effective = dcNotify;
        if (config.getSiteName() == null && dcNotify != DCNotify.ALL_DCS) {
            logger.debugf("Site name not configured; falling back to ALL_DCS delivery for task %s", taskKey);
            effective = DCNotify.ALL_DCS;
        }
        byte[] encoded = codec.encode(taskKey, events, ignoreSender, effective, nodeId, config.getSiteName());
        String payload = Base64.getEncoder().encodeToString(encoded);
        try {
            commandConnection.sync().publish(config.getChannel(), payload);
        } catch (RuntimeException ex) {
            logger.warnf(ex, "Failed to publish cluster event for task %s", taskKey);
            ValkeyMetrics.count("cluster", "notify", ValkeyMetrics.Outcome.ERROR);
        }
    }

    private void publishWorkCompletion(String lockKey, boolean success) {
        try {
            notify(WORK_COMPLETED_TASK_KEY, new ValkeyWorkCompletionEvent(lockKey, success), true);
        } catch (RuntimeException ex) {
            logger.debugf(ex, "Failed to publish work completion for %s", lockKey);
        }
    }
}
