package org.keycloak.valkey.workflow;

import java.util.Map;
import java.util.Objects;

import org.keycloak.models.workflow.WorkflowStateProvider;

/**
 * Serialized representation of a workflow state entry stored in Valkey.
 */
final class ValkeyWorkflowStateRecord {

    static final String FIELD_WORKFLOW_ID = "workflowId";
    static final String FIELD_WORKFLOW_PROVIDER_ID = "workflowProviderId";
    static final String FIELD_RESOURCE_ID = "resourceId";
    static final String FIELD_STEP_ID = "stepId";
    static final String FIELD_SCHEDULED_TIMESTAMP = "scheduledTimestamp";

    private final String executionId;
    private final String workflowId;
    private final String workflowProviderId;
    private final String resourceId;
    private final String stepId;
    private final long scheduledTimestamp;

    private ValkeyWorkflowStateRecord(String executionId, String workflowId, String workflowProviderId,
            String resourceId, String stepId, long scheduledTimestamp) {
        this.executionId = Objects.requireNonNull(executionId, "executionId");
        this.workflowId = Objects.requireNonNull(workflowId, "workflowId");
        this.workflowProviderId = workflowProviderId;
        this.resourceId = Objects.requireNonNull(resourceId, "resourceId");
        this.stepId = Objects.requireNonNull(stepId, "stepId");
        this.scheduledTimestamp = scheduledTimestamp;
    }

    static ValkeyWorkflowStateRecord create(String executionId, String workflowId, String workflowProviderId,
            String resourceId, String stepId, long scheduledTimestamp) {
        return new ValkeyWorkflowStateRecord(executionId, workflowId, workflowProviderId, resourceId, stepId,
                scheduledTimestamp);
    }

    static ValkeyWorkflowStateRecord fromMap(String executionId, Map<String, String> data) {
        if (data == null || data.isEmpty()) {
            return null;
        }
        String workflowId = data.get(FIELD_WORKFLOW_ID);
        String providerId = data.get(FIELD_WORKFLOW_PROVIDER_ID);
        String resourceId = data.get(FIELD_RESOURCE_ID);
        String stepId = data.get(FIELD_STEP_ID);
        String timestamp = data.get(FIELD_SCHEDULED_TIMESTAMP);
        if (workflowId == null || resourceId == null || stepId == null || timestamp == null) {
            return null;
        }
        try {
            long scheduled = Long.parseLong(timestamp);
            return new ValkeyWorkflowStateRecord(executionId, workflowId, providerId, resourceId, stepId, scheduled);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    WorkflowStateProvider.ScheduledStep toScheduledStep() {
        return new WorkflowStateProvider.ScheduledStep(workflowId, stepId, resourceId, executionId);
    }

    String executionId() {
        return executionId;
    }

    String workflowId() {
        return workflowId;
    }

    String workflowProviderId() {
        return workflowProviderId;
    }

    String resourceId() {
        return resourceId;
    }

    String stepId() {
        return stepId;
    }

    long scheduledTimestamp() {
        return scheduledTimestamp;
    }
}
