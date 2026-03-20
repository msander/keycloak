/*
 * Copyright 2026 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.keycloak.quarkus.runtime.services.health;

import jakarta.enterprise.context.ApplicationScoped;

import org.keycloak.services.resources.KeycloakApplication;

import io.smallrye.health.api.HealthGroup;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;

/**
 * Health group check exposed at {@code /health/group/active} on the management port.
 * <p>
 * Reports UP only when bootstrap has completed (session factory created, database
 * migration done, Infinispan started). This endpoint is intended for load balancer
 * health checks to control traffic routing.
 * <p>
 * In warm-standby deployments, pods report {@code readiness=UP} immediately so that
 * Kubernetes rolling updates can proceed, but the load balancer only routes traffic
 * to pods where this check returns UP.
 */
@HealthGroup("active")
@ApplicationScoped
public class KeycloakActiveHealthCheck implements HealthCheck {

    private boolean bootstrapCompleted;

    @Override
    public HealthCheckResponse call() {
        HealthCheckResponseBuilder builder = HealthCheckResponse.named("Keycloak active");
        if (bootstrapCompleted) {
            return builder.up().build();
        }
        if (KeycloakApplication.isBootstrapCompleted()) {
            bootstrapCompleted = true;
            return builder.up().build();
        }
        return builder.down().withData("status", "initializing").build();
    }
}
