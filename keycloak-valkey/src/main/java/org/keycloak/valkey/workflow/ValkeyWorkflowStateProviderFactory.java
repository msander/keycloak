package org.keycloak.valkey.workflow;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.logging.Logger;
import org.keycloak.Config;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.models.RealmModel.RealmRemovedEvent;
import org.keycloak.models.UserModel.UserRemovedEvent;
import org.keycloak.models.workflow.WorkflowStateProvider;
import org.keycloak.models.workflow.WorkflowStateProviderFactory;
import org.keycloak.provider.Provider;
import org.keycloak.provider.ProviderConfigProperty;
import org.keycloak.provider.ProviderConfigurationBuilder;
import org.keycloak.valkey.ValkeyConnectionProvider;

/**
 * Factory for the Valkey-backed {@link WorkflowStateProvider} implementation.
 */
public class ValkeyWorkflowStateProviderFactory implements WorkflowStateProviderFactory {

    public static final String PROVIDER_ID = "valkey";
    private static final Logger logger = Logger.getLogger(ValkeyWorkflowStateProviderFactory.class);
    private static final String CONFIG_NAMESPACE = "namespace";

    private volatile ValkeyWorkflowStateConfig config = ValkeyWorkflowStateConfig.from(null);

    @Override
    public WorkflowStateProvider create(KeycloakSession session) {
        ValkeyConnectionProvider connectionProvider = session.getProvider(ValkeyConnectionProvider.class);
        if (connectionProvider == null) {
            throw new IllegalStateException("ValkeyConnectionProvider is required for workflow state provider");
        }
        return new ValkeyWorkflowStateProvider(session, connectionProvider, config);
    }

    @Override
    public void init(Config.Scope scope) {
        this.config = ValkeyWorkflowStateConfig.from(scope);
        logger.debugf("Configured Valkey workflow state provider with namespace %s", config.getNamespace());
    }

    @Override
    public void postInit(KeycloakSessionFactory factory) {
        factory.register(event -> {
            if (event instanceof UserRemovedEvent userRemoved) {
                onUserRemoved(userRemoved);
            } else if (event instanceof RealmRemovedEvent realmRemoved) {
                onRealmRemoved(realmRemoved);
            }
        });
    }

    @Override
    public void close() {
        // No-op
    }

    @Override
    public String getId() {
        return PROVIDER_ID;
    }

    @Override
    public List<ProviderConfigProperty> getConfigMetadata() {
        ProviderConfigurationBuilder builder = ProviderConfigurationBuilder.create();
        builder.property()
                .name(CONFIG_NAMESPACE)
                .type(ProviderConfigProperty.STRING_TYPE)
                .defaultValue(ValkeyWorkflowStateConfig.DEFAULT_NAMESPACE)
                .helpText("Namespace prefix used for keys storing workflow state in Valkey.")
                .add();
        return builder.build();
    }

    public Set<Class<? extends Provider>> dependsOn() {
        return Set.of(ValkeyConnectionProvider.class);
    }

    private void onUserRemoved(UserRemovedEvent event) {
        KeycloakSession session = event.getKeycloakSession();
        WorkflowStateProvider provider = session.getProvider(WorkflowStateProvider.class, getId());
        if (provider != null) {
            provider.removeByResource(event.getUser().getId());
        }
    }

    private void onRealmRemoved(RealmRemovedEvent event) {
        KeycloakSession session = event.getKeycloakSession();
        WorkflowStateProvider provider = session.getProvider(WorkflowStateProvider.class, getId());
        if (provider != null) {
            provider.removeAll();
        }
    }
}
