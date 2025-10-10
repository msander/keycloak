package org.keycloak.valkey.workflow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.keycloak.common.util.Time;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.models.workflow.Workflow;
import org.keycloak.models.workflow.WorkflowStateProvider;
import org.keycloak.models.workflow.WorkflowStep;
import org.keycloak.valkey.ValkeyConnectionProvider;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.Range;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

/**
 * Valkey-backed implementation of {@link WorkflowStateProvider} using hashes and secondary indexes.
 */
final class ValkeyWorkflowStateProvider implements WorkflowStateProvider {

    private static final Comparator<ValkeyWorkflowStateRecord> BY_TIMESTAMP =
            Comparator.comparingLong(ValkeyWorkflowStateRecord::scheduledTimestamp)
                    .thenComparing(ValkeyWorkflowStateRecord::executionId);

    private final KeycloakSession session;
    private final RedisCommands<String, String> commands;
    private final ValkeyWorkflowStateConfig config;

    ValkeyWorkflowStateProvider(KeycloakSession session, ValkeyConnectionProvider connectionProvider,
            ValkeyWorkflowStateConfig config) {
        this.session = Objects.requireNonNull(session, "session");
        StatefulRedisConnection<String, String> connection = Objects
                .requireNonNull(connectionProvider, "connectionProvider")
                .getConnection();
        this.commands = connection.sync();
        this.config = Objects.requireNonNull(config, "config");
    }

    @Override
    public void scheduleStep(Workflow workflow, WorkflowStep step, String resourceId, String executionId) {
        Objects.requireNonNull(workflow, "workflow");
        Objects.requireNonNull(step, "step");
        Objects.requireNonNull(resourceId, "resourceId");
        Objects.requireNonNull(executionId, "executionId");
        RealmModel realm = requireRealm();
        String realmId = realm.getId();
        long delay = step.getAfter() != null ? step.getAfter() : 0L;
        long scheduledTimestamp = Time.currentTimeMillis() + delay;

        ValkeyWorkflowStateRecord record = ValkeyWorkflowStateRecord.create(executionId, workflow.getId(),
                workflow.getProviderId(), resourceId, step.getId(), scheduledTimestamp);

        String stateKey = stateKey(realmId, executionId);
        ValkeyWorkflowStateRecord existing = loadRecord(realmId, executionId);

        commands.multi();
        try {
            if (existing != null) {
                if (!existing.workflowId().equals(record.workflowId())) {
                    commands.zrem(workflowIndexKey(realmId, existing.workflowId()), executionId);
                }
                if (!existing.resourceId().equals(record.resourceId())) {
                    commands.srem(resourceIndexKey(realmId, existing.resourceId()), executionId);
                }
                if (!existing.stepId().equals(record.stepId())) {
                    commands.srem(stepIndexKey(realmId, existing.stepId()), executionId);
                }
            }
            commands.hset(stateKey, ValkeyWorkflowStateRecord.FIELD_WORKFLOW_ID, record.workflowId());
            if (record.workflowProviderId() != null && !record.workflowProviderId().isBlank()) {
                commands.hset(stateKey, ValkeyWorkflowStateRecord.FIELD_WORKFLOW_PROVIDER_ID,
                        record.workflowProviderId());
            } else {
                commands.hdel(stateKey, ValkeyWorkflowStateRecord.FIELD_WORKFLOW_PROVIDER_ID);
            }
            commands.hset(stateKey, ValkeyWorkflowStateRecord.FIELD_RESOURCE_ID, record.resourceId());
            commands.hset(stateKey, ValkeyWorkflowStateRecord.FIELD_STEP_ID, record.stepId());
            commands.hset(stateKey, ValkeyWorkflowStateRecord.FIELD_SCHEDULED_TIMESTAMP,
                    Long.toString(record.scheduledTimestamp()));
            commands.zadd(workflowIndexKey(realmId, record.workflowId()), scheduledTimestamp, executionId);
            commands.sadd(resourceIndexKey(realmId, record.resourceId()), executionId);
            commands.sadd(stepIndexKey(realmId, record.stepId()), executionId);
            commands.exec();
        } catch (RuntimeException ex) {
            safeDiscard();
            throw ex;
        }
    }

    @Override
    public WorkflowStateProvider.ScheduledStep getScheduledStep(String workflowId, String resourceId) {
        if (workflowId == null || workflowId.isBlank() || resourceId == null || resourceId.isBlank()) {
            return null;
        }
        RealmModel realm = requireRealm();
        String realmId = realm.getId();
        String indexKey = resourceIndexKey(realmId, resourceId);
        Set<String> executionIds = commands.smembers(indexKey);
        for (String executionId : executionIds) {
            ValkeyWorkflowStateRecord record = loadRecord(realmId, executionId);
            if (record == null) {
                commands.srem(indexKey, executionId);
                continue;
            }
            if (workflowId.equals(record.workflowId())) {
                return record.toScheduledStep();
            }
        }
        return null;
    }

    @Override
    public List<WorkflowStateProvider.ScheduledStep> getScheduledStepsByResource(String resourceId) {
        if (resourceId == null || resourceId.isBlank()) {
            return List.of();
        }
        RealmModel realm = requireRealm();
        String realmId = realm.getId();
        String indexKey = resourceIndexKey(realmId, resourceId);
        Set<String> executionIds = commands.smembers(indexKey);
        List<ValkeyWorkflowStateRecord> records = new ArrayList<>(executionIds.size());
        for (String executionId : executionIds) {
            ValkeyWorkflowStateRecord record = loadRecord(realmId, executionId);
            if (record == null) {
                commands.srem(indexKey, executionId);
                continue;
            }
            records.add(record);
        }
        records.sort(BY_TIMESTAMP);
        return toScheduledSteps(records);
    }

    @Override
    public List<WorkflowStateProvider.ScheduledStep> getScheduledStepsByWorkflow(String workflowId) {
        if (workflowId == null || workflowId.isBlank()) {
            return List.of();
        }
        RealmModel realm = requireRealm();
        String realmId = realm.getId();
        String indexKey = workflowIndexKey(realmId, workflowId);
        List<String> executionIds = commands.zrange(indexKey, 0, -1);
        List<ValkeyWorkflowStateRecord> records = new ArrayList<>(executionIds.size());
        for (String executionId : executionIds) {
            ValkeyWorkflowStateRecord record = loadRecord(realmId, executionId);
            if (record == null) {
                commands.zrem(indexKey, executionId);
                continue;
            }
            records.add(record);
        }
        return toScheduledSteps(records);
    }

    @Override
    public List<WorkflowStateProvider.ScheduledStep> getScheduledStepsByWorkflow(Workflow workflow) {
        if (workflow == null) {
            return List.of();
        }
        return getScheduledStepsByWorkflow(workflow.getId());
    }

    @Override
    public List<WorkflowStateProvider.ScheduledStep> getScheduledStepsByStep(String stepId) {
        if (stepId == null || stepId.isBlank()) {
            return List.of();
        }
        RealmModel realm = requireRealm();
        String realmId = realm.getId();
        String indexKey = stepIndexKey(realmId, stepId);
        Set<String> executionIds = commands.smembers(indexKey);
        List<ValkeyWorkflowStateRecord> records = new ArrayList<>(executionIds.size());
        for (String executionId : executionIds) {
            ValkeyWorkflowStateRecord record = loadRecord(realmId, executionId);
            if (record == null) {
                commands.srem(indexKey, executionId);
                continue;
            }
            records.add(record);
        }
        records.sort(BY_TIMESTAMP);
        return toScheduledSteps(records);
    }

    @Override
    public List<WorkflowStateProvider.ScheduledStep> getDueScheduledSteps(Workflow workflow) {
        if (workflow == null) {
            return List.of();
        }
        RealmModel realm = requireRealm();
        String realmId = realm.getId();
        String indexKey = workflowIndexKey(realmId, workflow.getId());
        long now = Time.currentTimeMillis();
        List<String> dueExecutions = commands.zrangebyscore(indexKey, Range.create(0.0, (double) now));
        List<ValkeyWorkflowStateRecord> records = new ArrayList<>(dueExecutions.size());
        for (String executionId : dueExecutions) {
            ValkeyWorkflowStateRecord record = loadRecord(realmId, executionId);
            if (record == null) {
                commands.zrem(indexKey, executionId);
                continue;
            }
            records.add(record);
        }
        return toScheduledSteps(records);
    }

    @Override
    public void removeByResource(String resourceId) {
        if (resourceId == null || resourceId.isBlank()) {
            return;
        }
        RealmModel realm = requireRealm();
        String realmId = realm.getId();
        String indexKey = resourceIndexKey(realmId, resourceId);
        Set<String> executionIds = commands.smembers(indexKey);
        for (String executionId : executionIds) {
            removeExecution(realmId, executionId);
        }
        commands.del(indexKey);
    }

    @Override
    public void removeByWorkflow(String workflowId) {
        if (workflowId == null || workflowId.isBlank()) {
            return;
        }
        RealmModel realm = requireRealm();
        String realmId = realm.getId();
        String indexKey = workflowIndexKey(realmId, workflowId);
        List<String> executionIds = commands.zrange(indexKey, 0, -1);
        for (String executionId : executionIds) {
            removeExecution(realmId, executionId);
        }
        commands.del(indexKey);
    }

    @Override
    public void remove(String executionId) {
        if (executionId == null || executionId.isBlank()) {
            return;
        }
        RealmModel realm = requireRealm();
        removeExecution(realm.getId(), executionId);
    }

    @Override
    public void removeAll() {
        RealmModel realm = requireRealm();
        String realmId = realm.getId();
        String statePattern = stateKeyPrefix(realmId) + '*';
        scanAndApply(statePattern, key -> {
            String executionId = key.substring(stateKeyPrefix(realmId).length());
            removeExecution(realmId, executionId);
        });
        deleteByPattern(workflowIndexPrefix(realmId) + '*');
        deleteByPattern(resourceIndexPrefix(realmId) + '*');
        deleteByPattern(stepIndexPrefix(realmId) + '*');
    }

    @Override
    public void close() {
        // Connection lifecycle managed by the ValkeyConnectionProvider.
    }

    private List<WorkflowStateProvider.ScheduledStep> toScheduledSteps(
            Collection<ValkeyWorkflowStateRecord> records) {
        if (records.isEmpty()) {
            return List.of();
        }
        List<WorkflowStateProvider.ScheduledStep> steps = new ArrayList<>(records.size());
        for (ValkeyWorkflowStateRecord record : records) {
            steps.add(record.toScheduledStep());
        }
        return Collections.unmodifiableList(steps);
    }

    private void removeExecution(String realmId, String executionId) {
        ValkeyWorkflowStateRecord record = loadRecord(realmId, executionId);
        String stateKey = stateKey(realmId, executionId);
        if (record == null) {
            commands.del(stateKey);
            cleanupOrphanedIndexes(realmId, executionId);
            return;
        }
        commands.multi();
        try {
            commands.del(stateKey);
            commands.zrem(workflowIndexKey(realmId, record.workflowId()), executionId);
            commands.srem(resourceIndexKey(realmId, record.resourceId()), executionId);
            commands.srem(stepIndexKey(realmId, record.stepId()), executionId);
            commands.exec();
        } catch (RuntimeException ex) {
            safeDiscard();
            throw ex;
        }
    }

    private void cleanupOrphanedIndexes(String realmId, String executionId) {
        scanAndApply(workflowIndexPrefix(realmId) + '*', key -> commands.zrem(key, executionId));
        scanAndApply(resourceIndexPrefix(realmId) + '*', key -> commands.srem(key, executionId));
        scanAndApply(stepIndexPrefix(realmId) + '*', key -> commands.srem(key, executionId));
    }

    private ValkeyWorkflowStateRecord loadRecord(String realmId, String executionId) {
        String key = stateKey(realmId, executionId);
        Map<String, String> data = commands.hgetall(key);
        if (data == null || data.isEmpty()) {
            return null;
        }
        ValkeyWorkflowStateRecord record = ValkeyWorkflowStateRecord.fromMap(executionId, data);
        if (record == null) {
            commands.del(key);
        }
        return record;
    }

    private RealmModel requireRealm() {
        RealmModel realm = session.getContext() != null ? session.getContext().getRealm() : null;
        if (realm == null) {
            throw new IllegalStateException("Realm must be set in the session context for workflow state operations");
        }
        return realm;
    }

    private String stateKey(String realmId, String executionId) {
        return stateKeyPrefix(realmId) + executionId;
    }

    private String stateKeyPrefix(String realmId) {
        return config.getNamespace() + ":state:" + realmId + ':';
    }

    private String workflowIndexKey(String realmId, String workflowId) {
        return workflowIndexPrefix(realmId) + workflowId;
    }

    private String workflowIndexPrefix(String realmId) {
        return config.getNamespace() + ":workflow:" + realmId + ':';
    }

    private String resourceIndexKey(String realmId, String resourceId) {
        return resourceIndexPrefix(realmId) + resourceId;
    }

    private String resourceIndexPrefix(String realmId) {
        return config.getNamespace() + ":resource:" + realmId + ':';
    }

    private String stepIndexKey(String realmId, String stepId) {
        return stepIndexPrefix(realmId) + stepId;
    }

    private String stepIndexPrefix(String realmId) {
        return config.getNamespace() + ":step:" + realmId + ':';
    }

    private void deleteByPattern(String pattern) {
        scanAndApply(pattern, commands::del);
    }

    private void scanAndApply(String pattern, java.util.function.Consumer<String> action) {
        ScanCursor cursor = ScanCursor.INITIAL;
        ScanArgs args = ScanArgs.Builder.matches(pattern).limit(200);
        do {
            KeyScanCursor<String> scan = commands.scan(cursor, args);
            for (String key : scan.getKeys()) {
                action.accept(key);
            }
            cursor = scan;
        } while (!cursor.isFinished());
    }

    private void safeDiscard() {
        try {
            commands.discard();
        } catch (RuntimeException ignore) {
            // Transaction was not active; nothing to discard.
        }
    }
}
