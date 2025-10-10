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
import org.keycloak.storage.DatastoreProvider;
import org.keycloak.storage.ExportImportManager;
import org.keycloak.storage.StoreManagers;
import org.keycloak.storage.datastore.DefaultDatastoreProvider;

/**
 * Wrapper around the default datastore provider that prefers Valkey-backed implementations when present.
 */
final class ValkeyDatastoreProvider implements DatastoreProvider, StoreManagers {

    private final DefaultDatastoreProvider delegate;
    private final KeycloakSession session;
    private final ConcurrentMap<Class<?>, Object> overrides = new ConcurrentHashMap<>();

    ValkeyDatastoreProvider(DefaultDatastoreProvider delegate, KeycloakSession session) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.session = Objects.requireNonNull(session, "session");
    }

    @Override
    public AuthenticationSessionProvider authSessions() {
        return prefer(AuthenticationSessionProvider.class, delegate::authSessions);
    }

    @Override
    public ClientScopeProvider clientScopes() {
        return prefer(ClientScopeProvider.class, delegate::clientScopes);
    }

    @Override
    public ClientProvider clients() {
        return prefer(ClientProvider.class, delegate::clients);
    }

    @Override
    public GroupProvider groups() {
        return prefer(GroupProvider.class, delegate::groups);
    }

    @Override
    public IdentityProviderStorageProvider identityProviders() {
        return prefer(IdentityProviderStorageProvider.class, delegate::identityProviders);
    }

    @Override
    public UserLoginFailureProvider loginFailures() {
        return prefer(UserLoginFailureProvider.class, delegate::loginFailures);
    }

    @Override
    public RealmProvider realms() {
        return prefer(RealmProvider.class, delegate::realms);
    }

    @Override
    public RoleProvider roles() {
        return prefer(RoleProvider.class, delegate::roles);
    }

    @Override
    public SingleUseObjectProvider singleUseObjects() {
        return prefer(SingleUseObjectProvider.class, delegate::singleUseObjects);
    }

    @Override
    public UserProvider users() {
        return prefer(UserProvider.class, delegate::users);
    }

    @Override
    public UserSessionProvider userSessions() {
        return prefer(UserSessionProvider.class, delegate::userSessions);
    }

    @Override
    public ExportImportManager getExportImportManager() {
        return delegate.getExportImportManager();
    }

    @Override
    public ClientProvider clientStorageManager() {
        return delegate.clientStorageManager();
    }

    @Override
    public ClientScopeProvider clientScopeStorageManager() {
        return delegate.clientScopeStorageManager();
    }

    @Override
    public RoleProvider roleStorageManager() {
        return delegate.roleStorageManager();
    }

    @Override
    public GroupProvider groupStorageManager() {
        return delegate.groupStorageManager();
    }

    @Override
    public UserProvider userStorageManager() {
        return delegate.userStorageManager();
    }

    @Override
    public UserProvider userLocalStorage() {
        return delegate.userLocalStorage();
    }

    @Override
    public org.keycloak.storage.federated.UserFederatedStorageProvider userFederatedStorage() {
        return delegate.userFederatedStorage();
    }

    @Override
    public void close() {
        overrides.clear();
        delegate.close();
    }

    @SuppressWarnings("unchecked")
    private <T extends Provider> T prefer(Class<T> providerClass, Supplier<T> fallback) {
        Object existing = overrides.get(providerClass);
        if (existing != null) {
            return (T) existing;
        }
        Object resolved = overrides.computeIfAbsent(providerClass, key -> {
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
