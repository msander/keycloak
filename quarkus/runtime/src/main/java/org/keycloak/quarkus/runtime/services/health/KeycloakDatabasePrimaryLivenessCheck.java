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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.keycloak.services.resources.KeycloakApplication;

import io.agroal.api.AgroalDataSource;
import io.quarkus.runtime.Quarkus;
import io.quarkus.smallrye.health.runtime.QuarkusAsyncHealthCheckFactory;
import io.smallrye.health.api.AsyncHealthCheck;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;
import org.eclipse.microprofile.health.Liveness;
import org.jboss.logging.Logger;

@Liveness
@ApplicationScoped
public class KeycloakDatabasePrimaryLivenessCheck implements AsyncHealthCheck {

    private static final Logger logger = Logger.getLogger(KeycloakDatabasePrimaryLivenessCheck.class);

    private final AtomicBoolean shutdownRequested = new AtomicBoolean();

    @Inject
    AgroalDataSource agroalDataSource;

    @Inject
    QuarkusAsyncHealthCheckFactory healthCheckFactory;

    @Override
    public Uni<HealthCheckResponse> call() {
        if (!KeycloakApplication.isBootstrapCompleted()) {
            return healthCheckFactory.callAsync(() -> Uni.createFrom()
                    .item(HealthCheckResponse.named("Keycloak database primary liveness check").up().build()));
        }

        return healthCheckFactory.callSync(() -> {
            HealthCheckResponseBuilder builder = HealthCheckResponse.named("Keycloak database primary liveness check");
            try (Connection connection = agroalDataSource.getConnection()) {
                // During a real failover the server kills all connections. Agroal detects
                // the dead connections and creates new ones, which triggers the JDBC
                // driver's targetServerType=primary check. If the database has been
                // demoted to a replica, the driver throws PSQLException and the catch
                // block below initiates shutdown.
                //
                // isValid() sends a validation query to detect connections that are
                // still in the pool but no longer usable (e.g. TCP half-open after
                // a network partition). Without this, getConnection() alone may return
                // a stale connection that appears healthy.
                connection.isValid(5);
                return builder.up().build();
            } catch (SQLException e) {
                if (KeycloakApplication.isTargetServerTypeRejection(e)) {
                    if (shutdownRequested.compareAndSet(false, true)) {
                        logger.errorf("Database is no longer primary: %s. Requesting server shutdown.", e.getMessage());
                        Quarkus.asyncExit(1);
                    }
                    return builder.down().withData("Reason", e.getMessage()).build();
                }
                return builder.up().withData("Reason", "Unable to determine database status: " + e.getMessage()).build();
            }
        });
    }
}
