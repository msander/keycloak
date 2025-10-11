package org.keycloak.valkey.datastore;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import org.keycloak.models.ClientProvider;
import org.keycloak.models.ClientScopeProvider;
import org.keycloak.models.GroupProvider;
import org.keycloak.models.IdentityProviderStorageProvider;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmProvider;
import org.keycloak.models.RoleProvider;
import org.keycloak.models.SingleUseObjectProvider;
import org.keycloak.models.UserLoginFailureProvider;
import org.keycloak.models.UserProvider;
import org.keycloak.models.UserSessionProvider;
import org.keycloak.provider.Provider;
import org.keycloak.sessions.AuthenticationSessionProvider;
import org.keycloak.storage.datastore.DefaultDatastoreProvider;
import org.keycloak.storage.datastore.DefaultDatastoreProviderFactory;

/**
 * {@link DefaultDatastoreProvider} variant that prefers Valkey-backed implementations when present.
 */
final class ValkeyDatastoreProvider extends DefaultDatastoreProvider {

    private final KeycloakSession session;
    private final ConcurrentMap<Class<?>, Provider> overrides = new ConcurrentHashMap<>();

    ValkeyDatastoreProvider(DefaultDatastoreProviderFactory factory, KeycloakSession session) {
        super(Objects.requireNonNull(factory, "factory"), Objects.requireNonNull(session, "session"));
        this.session = session;
    }

    @Override
    public AuthenticationSessionProvider authSessions() {
        return prefer(AuthenticationSessionProvider.class, super::authSessions);
    }

    @Override
    public ClientScopeProvider clientScopes() {
        return prefer(ClientScopeProvider.class, super::clientScopes);
    }

    @Override
    public ClientProvider clients() {
        return prefer(ClientProvider.class, super::clients);
    }

    @Override
    public GroupProvider groups() {
        return prefer(GroupProvider.class, super::groups);
    }

    @Override
    public IdentityProviderStorageProvider identityProviders() {
        return prefer(IdentityProviderStorageProvider.class, super::identityProviders);
    }

    @Override
    public UserLoginFailureProvider loginFailures() {
        return prefer(UserLoginFailureProvider.class, super::loginFailures);
    }

    @Override
    public RealmProvider realms() {
        return prefer(RealmProvider.class, super::realms);
    }

    @Override
    public RoleProvider roles() {
        return prefer(RoleProvider.class, super::roles);
    }

    @Override
    public SingleUseObjectProvider singleUseObjects() {
        return prefer(SingleUseObjectProvider.class, super::singleUseObjects);
    }

    @Override
    public UserProvider users() {
        return prefer(UserProvider.class, super::users);
    }

    @Override
    public UserSessionProvider userSessions() {
        return prefer(UserSessionProvider.class, super::userSessions);
    }

    @Override
    public void close() {
        overrides.clear();
        super.close();
    }

    @SuppressWarnings("unchecked")
    private <T extends Provider> T prefer(Class<T> providerClass, Supplier<T> fallback) {
        Provider cached = overrides.get(providerClass);
        if (cached != null) {
            return (T) cached;
        }

        Provider resolved = overrides.computeIfAbsent(providerClass, key -> {
            T valkeyProvider = session.getProvider(providerClass, ValkeyDatastoreProviderFactory.PROVIDER_ID);
            if (valkeyProvider != null) {
                return valkeyProvider;
            }
            T fallbackProvider = fallback.get();
            if (fallbackProvider == null) {
                throw new IllegalStateException("No provider available for " + providerClass.getName());
            }
            return fallbackProvider;
        });

        return (T) resolved;
    }
}
