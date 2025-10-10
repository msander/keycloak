package org.keycloak.valkey.datastore;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
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
import org.keycloak.sessions.AuthenticationSessionProvider;
import org.keycloak.storage.datastore.DefaultDatastoreProvider;

class ValkeyDatastoreProviderTest {

    private final KeycloakSession session = mock(KeycloakSession.class);
    private final DefaultDatastoreProvider delegate = mock(DefaultDatastoreProvider.class);
    private final ValkeyDatastoreProvider provider = new ValkeyDatastoreProvider(delegate, session);

    @Test
    void prefersValkeyProvidersWhenAvailable() {
        AuthenticationSessionProvider authProvider = mock(AuthenticationSessionProvider.class);
        when(session.getProvider(eq(AuthenticationSessionProvider.class), eq(ValkeyDatastoreProviderFactory.PROVIDER_ID)))
                .thenReturn(authProvider);

        assertSame(authProvider, provider.authSessions());
        verifyNoInteractions(delegate);
    }

    @Test
    void fallsBackToDelegateWhenValkeyProviderMissing() {
        AuthenticationSessionProvider fallback = mock(AuthenticationSessionProvider.class);
        when(delegate.authSessions()).thenReturn(fallback);

        assertSame(fallback, provider.authSessions());
        verify(delegate, times(1)).authSessions();
    }

    @Test
    void cachesResolvedProviders() {
        AuthenticationSessionProvider fallback = mock(AuthenticationSessionProvider.class);
        when(delegate.authSessions()).thenReturn(fallback);

        provider.authSessions();
        provider.authSessions();

        verify(delegate, times(1)).authSessions();
    }

    @Test
    void clearsOverridesOnClose() {
        AuthenticationSessionProvider valkeyAuth = mock(AuthenticationSessionProvider.class);
        when(session.getProvider(eq(AuthenticationSessionProvider.class), eq(ValkeyDatastoreProviderFactory.PROVIDER_ID)))
                .thenReturn(valkeyAuth);
        provider.authSessions();

        provider.close();

        AuthenticationSessionProvider fallback = mock(AuthenticationSessionProvider.class);
        when(session.getProvider(eq(AuthenticationSessionProvider.class), eq(ValkeyDatastoreProviderFactory.PROVIDER_ID)))
                .thenReturn(null);
        when(delegate.authSessions()).thenReturn(fallback);

        assertSame(fallback, provider.authSessions());
    }

    @Test
    void delegatesStoreManagerAccessors() {
        ClientProvider clientProvider = mock(ClientProvider.class);
        ClientScopeProvider clientScopeProvider = mock(ClientScopeProvider.class);
        RoleProvider roleProvider = mock(RoleProvider.class);
        GroupProvider groupProvider = mock(GroupProvider.class);
        UserProvider userProvider = mock(UserProvider.class);
        org.keycloak.storage.federated.UserFederatedStorageProvider federated =
                mock(org.keycloak.storage.federated.UserFederatedStorageProvider.class);

        when(delegate.clientStorageManager()).thenReturn(clientProvider);
        when(delegate.clientScopeStorageManager()).thenReturn(clientScopeProvider);
        when(delegate.roleStorageManager()).thenReturn(roleProvider);
        when(delegate.groupStorageManager()).thenReturn(groupProvider);
        when(delegate.userStorageManager()).thenReturn(userProvider);
        when(delegate.userLocalStorage()).thenReturn(userProvider);
        when(delegate.userFederatedStorage()).thenReturn(federated);

        assertSame(clientProvider, provider.clientStorageManager());
        assertSame(clientScopeProvider, provider.clientScopeStorageManager());
        assertSame(roleProvider, provider.roleStorageManager());
        assertSame(groupProvider, provider.groupStorageManager());
        assertSame(userProvider, provider.userStorageManager());
        assertSame(userProvider, provider.userLocalStorage());
        assertSame(federated, provider.userFederatedStorage());
    }

    @Test
    void prefersValkeyForAllCoreProviders() {
        ClientProvider clientProvider = mock(ClientProvider.class);
        ClientScopeProvider clientScopeProvider = mock(ClientScopeProvider.class);
        GroupProvider groupProvider = mock(GroupProvider.class);
        IdentityProviderStorageProvider idpProvider = mock(IdentityProviderStorageProvider.class);
        UserLoginFailureProvider loginFailureProvider = mock(UserLoginFailureProvider.class);
        RealmProvider realmProvider = mock(RealmProvider.class);
        RoleProvider roleProvider = mock(RoleProvider.class);
        SingleUseObjectProvider singleUseProvider = mock(SingleUseObjectProvider.class);
        UserProvider userProvider = mock(UserProvider.class);
        UserSessionProvider userSessionProvider = mock(UserSessionProvider.class);

        when(session.getProvider(eq(ClientProvider.class), eq(ValkeyDatastoreProviderFactory.PROVIDER_ID)))
                .thenReturn(clientProvider);
        when(session.getProvider(eq(ClientScopeProvider.class), eq(ValkeyDatastoreProviderFactory.PROVIDER_ID)))
                .thenReturn(clientScopeProvider);
        when(session.getProvider(eq(GroupProvider.class), eq(ValkeyDatastoreProviderFactory.PROVIDER_ID)))
                .thenReturn(groupProvider);
        when(session.getProvider(eq(IdentityProviderStorageProvider.class), eq(ValkeyDatastoreProviderFactory.PROVIDER_ID)))
                .thenReturn(idpProvider);
        when(session.getProvider(eq(UserLoginFailureProvider.class), eq(ValkeyDatastoreProviderFactory.PROVIDER_ID)))
                .thenReturn(loginFailureProvider);
        when(session.getProvider(eq(RealmProvider.class), eq(ValkeyDatastoreProviderFactory.PROVIDER_ID)))
                .thenReturn(realmProvider);
        when(session.getProvider(eq(RoleProvider.class), eq(ValkeyDatastoreProviderFactory.PROVIDER_ID)))
                .thenReturn(roleProvider);
        when(session.getProvider(eq(SingleUseObjectProvider.class), eq(ValkeyDatastoreProviderFactory.PROVIDER_ID)))
                .thenReturn(singleUseProvider);
        when(session.getProvider(eq(UserProvider.class), eq(ValkeyDatastoreProviderFactory.PROVIDER_ID)))
                .thenReturn(userProvider);
        when(session.getProvider(eq(UserSessionProvider.class), eq(ValkeyDatastoreProviderFactory.PROVIDER_ID)))
                .thenReturn(userSessionProvider);

        assertSame(clientProvider, provider.clients());
        assertSame(clientScopeProvider, provider.clientScopes());
        assertSame(groupProvider, provider.groups());
        assertSame(idpProvider, provider.identityProviders());
        assertSame(loginFailureProvider, provider.loginFailures());
        assertSame(realmProvider, provider.realms());
        assertSame(roleProvider, provider.roles());
        assertSame(singleUseProvider, provider.singleUseObjects());
        assertSame(userProvider, provider.users());
        assertSame(userSessionProvider, provider.userSessions());
    }
}
