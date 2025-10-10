package org.keycloak.valkey.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.keycloak.common.util.MultivaluedHashMap;
import org.keycloak.component.ComponentModel;
import org.keycloak.models.KeycloakContext;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.models.workflow.Workflow;
import org.keycloak.models.workflow.WorkflowStateProvider;
import org.keycloak.models.workflow.WorkflowStep;
import org.keycloak.valkey.ValkeyConnectionProvider;
import org.keycloak.valkey.connection.LettuceValkeyConnectionProviderFactory;
import org.keycloak.valkey.testing.EmbeddedValkeyServer;
import org.keycloak.valkey.testing.MapBackedConfigScope;
import org.mockito.Mockito;

class ValkeyWorkflowStateProviderTest {

    private static EmbeddedValkeyServer server;
    private static LettuceValkeyConnectionProviderFactory connectionFactory;

    private KeycloakSession session;
    private ValkeyConnectionProvider connectionProvider;
    private RealmModel realm;
    private ValkeyWorkflowStateProvider provider;

    @BeforeAll
    static void startServer() {
        server = EmbeddedValkeyServer.builder()
                .startupTimeout(Duration.ofSeconds(5))
                .start();
        connectionFactory = new LettuceValkeyConnectionProviderFactory();
        Map<String, String> config = Map.of("uri", "redis://" + server.getHost() + ':' + server.getPort());
        connectionFactory.init(MapBackedConfigScope.from(config));
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
        connectionProvider = connectionFactory.create(Mockito.mock(KeycloakSession.class));
        connectionProvider.getConnection().sync().flushall();

        session = Mockito.mock(KeycloakSession.class);
        KeycloakContext context = Mockito.mock(KeycloakContext.class);
        Mockito.when(session.getContext()).thenReturn(context);

        realm = Mockito.mock(RealmModel.class);
        Mockito.when(realm.getId()).thenReturn("realm-1");
        Mockito.when(context.getRealm()).thenReturn(realm);
        Mockito.when(session.getProvider(ValkeyConnectionProvider.class)).thenReturn(connectionProvider);

        provider = new ValkeyWorkflowStateProvider(session, connectionProvider, ValkeyWorkflowStateConfig.from(null));
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
    void shouldScheduleAndRetrieveState() {
        Workflow workflow = workflow("workflow-1", "provider-1");
        WorkflowStep step = step("step-1", 0L);

        provider.scheduleStep(workflow, step, "user-1", "exec-1");

        WorkflowStateProvider.ScheduledStep scheduled = provider.getScheduledStep(workflow.getId(), "user-1");
        assertNotNull(scheduled);
        assertEquals(workflow.getId(), scheduled.workflowId());
        assertEquals(step.getId(), scheduled.stepId());
        assertEquals("user-1", scheduled.resourceId());
        assertEquals("exec-1", scheduled.executionId());

        List<WorkflowStateProvider.ScheduledStep> byResource = provider.getScheduledStepsByResource("user-1");
        assertEquals(1, byResource.size());

        List<WorkflowStateProvider.ScheduledStep> byWorkflow = provider.getScheduledStepsByWorkflow(workflow);
        assertEquals(1, byWorkflow.size());

        List<WorkflowStateProvider.ScheduledStep> byStep = provider.getScheduledStepsByStep(step.getId());
        assertEquals(1, byStep.size());

        List<WorkflowStateProvider.ScheduledStep> due = provider.getDueScheduledSteps(workflow);
        assertEquals(1, due.size());
        assertEquals("exec-1", due.get(0).executionId());
    }

    @Test
    void shouldUpdateIndexesWhenRescheduling() {
        Workflow workflow = workflow("workflow-idx", "provider-idx");
        WorkflowStep step = step("step-idx", 0L);

        provider.scheduleStep(workflow, step, "user-old", "exec-idx");
        provider.scheduleStep(workflow, step, "user-new", "exec-idx");

        assertTrue(provider.getScheduledStepsByResource("user-old").isEmpty());
        List<WorkflowStateProvider.ScheduledStep> updated = provider.getScheduledStepsByResource("user-new");
        assertEquals(1, updated.size());
        assertEquals("user-new", updated.get(0).resourceId());
    }

    @Test
    void shouldRemoveByWorkflowAndExecution() {
        Workflow workflowA = workflow("workflow-A", "provider-A");
        Workflow workflowB = workflow("workflow-B", "provider-B");
        WorkflowStep stepA = step("step-A", 0L);
        WorkflowStep stepB = step("step-B", 0L);

        provider.scheduleStep(workflowA, stepA, "user-A", "exec-A");
        provider.scheduleStep(workflowB, stepB, "user-B", "exec-B");

        provider.removeByWorkflow(workflowA.getId());

        assertTrue(provider.getScheduledStepsByWorkflow(workflowA).isEmpty());
        assertEquals(1, provider.getScheduledStepsByWorkflow(workflowB).size());

        provider.remove("exec-B");
        assertTrue(provider.getScheduledStepsByWorkflow(workflowB).isEmpty());
    }

    @Test
    void shouldRemoveAllState() {
        Workflow workflow = workflow("workflow-clear", "provider-clear");
        WorkflowStep step1 = step("step-clear-1", 0L);
        WorkflowStep step2 = step("step-clear-2", 0L);

        provider.scheduleStep(workflow, step1, "user-1", "exec-clear-1");
        provider.scheduleStep(workflow, step2, "user-2", "exec-clear-2");

        provider.removeAll();

        assertTrue(provider.getScheduledStepsByWorkflow(workflow).isEmpty());
        assertTrue(provider.getScheduledStepsByResource("user-1").isEmpty());
        assertTrue(provider.getScheduledStepsByResource("user-2").isEmpty());
    }

    private Workflow workflow(String id, String providerId) {
        ComponentModel component = new ComponentModel();
        component.setId(id);
        component.setProviderId(providerId);
        component.setConfig(new MultivaluedHashMap<>());
        return new Workflow(component);
    }

    private WorkflowStep step(String id, long afterMillis) {
        ComponentModel component = new ComponentModel();
        component.setId(id);
        component.setProviderId("step-provider");
        component.setConfig(new MultivaluedHashMap<>());
        WorkflowStep step = new WorkflowStep(component);
        step.setAfter(afterMillis);
        return step;
    }
}
