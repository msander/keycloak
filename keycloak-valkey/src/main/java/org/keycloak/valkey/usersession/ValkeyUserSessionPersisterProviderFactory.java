package org.keycloak.valkey.usersession;

import java.util.Objects;

import org.keycloak.Config;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.models.session.UserSessionPersisterProvider;
import org.keycloak.models.session.UserSessionPersisterProviderFactory;
import org.keycloak.valkey.ValkeyConnectionProvider;

public final class ValkeyUserSessionPersisterProviderFactory implements UserSessionPersisterProviderFactory {

    public static final String PROVIDER_ID = "valkey";

    @Override
    public UserSessionPersisterProvider create(KeycloakSession session) {
        ValkeyConnectionProvider connectionProvider = session.getProvider(ValkeyConnectionProvider.class);
        Objects.requireNonNull(connectionProvider, "ValkeyConnectionProvider is required");
        return new ValkeyUserSessionPersisterProvider(session, connectionProvider);
    }

    @Override
    public void init(Config.Scope config) {
        // no-op
    }

    @Override
    public void postInit(KeycloakSessionFactory factory) {
        // no-op
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    public String getId() {
        return PROVIDER_ID;
    }
}
