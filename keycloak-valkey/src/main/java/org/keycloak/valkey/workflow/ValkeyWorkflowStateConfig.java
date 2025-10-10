package org.keycloak.valkey.workflow;

import java.util.Objects;

import org.keycloak.Config;

/**
 * Configuration options for the Valkey workflow state provider.
 */
final class ValkeyWorkflowStateConfig {

    static final String DEFAULT_NAMESPACE = "keycloak:workflow-state";

    private final String namespace;

    private ValkeyWorkflowStateConfig(String namespace) {
        this.namespace = validateNamespace(namespace);
    }

    String getNamespace() {
        return namespace;
    }

    static ValkeyWorkflowStateConfig from(Config.Scope scope) {
        if (scope == null) {
            return new ValkeyWorkflowStateConfig(DEFAULT_NAMESPACE);
        }
        String configured = scope.get("namespace");
        if (configured == null || configured.isBlank()) {
            return new ValkeyWorkflowStateConfig(DEFAULT_NAMESPACE);
        }
        return new ValkeyWorkflowStateConfig(configured.trim());
    }

    private static String validateNamespace(String candidate) {
        String value = Objects.requireNonNull(candidate, "namespace");
        if (value.isBlank()) {
            throw new IllegalArgumentException("Valkey workflow state namespace must not be blank");
        }
        return value;
    }
}
