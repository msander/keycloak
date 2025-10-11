package org.keycloak.valkey.datastore;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
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
import org.keycloak.storage.datastore.DefaultDatastoreProviderFactory;

class ValkeyDatastoreProviderTest {

    private KeycloakSession session;
    private ValkeyDatastoreProvider provider;

    @BeforeEach
    void setUp() {
        session = mock(KeycloakSession.class);
        provider = new ValkeyDatastoreProvider(new DefaultDatastoreProviderFactory(), session);
    }

    @Test
    void prefersValkeyProvidersWhenAvailable() {
        AuthenticationSessionProvider valkeyProvider = mock(AuthenticationSessionProvider.class);
        when(session.getProvider(eq(AuthenticationSessionProvider.class),
                eq(ValkeyDatastoreProviderFactory.PROVIDER_ID))).thenReturn(valkeyProvider);

        AuthenticationSessionProvider fallback = mock(AuthenticationSessionProvider.class);
        when(session.getProvider(AuthenticationSessionProvider.class)).thenReturn(fallback);

        assertSame(valkeyProvider, provider.authSessions());
        verify(session, times(1)).getProvider(eq(AuthenticationSessionProvider.class),
                eq(ValkeyDatastoreProviderFactory.PROVIDER_ID));
        verify(session, times(0)).getProvider(AuthenticationSessionProvider.class);
        verifyNoMoreInteractions(session);
    }

    @Test
    void fallsBackToDefaultWhenValkeyProviderMissing() {
        when(session.getProvider(eq(AuthenticationSessionProvider.class),
                eq(ValkeyDatastoreProviderFactory.PROVIDER_ID))).thenReturn(null);

        AuthenticationSessionProvider fallback = mock(AuthenticationSessionProvider.class);
        when(session.getProvider(AuthenticationSessionProvider.class)).thenReturn(fallback);

        assertSame(fallback, provider.authSessions());
        verify(session, times(1)).getProvider(eq(AuthenticationSessionProvider.class),
                eq(ValkeyDatastoreProviderFactory.PROVIDER_ID));
        verify(session, times(1)).getProvider(AuthenticationSessionProvider.class);
        verifyNoMoreInteractions(session);
    }

    @Test
    void cachesResolvedProviders() {
        when(session.getProvider(eq(AuthenticationSessionProvider.class),
                eq(ValkeyDatastoreProviderFactory.PROVIDER_ID))).thenReturn(null);
        AuthenticationSessionProvider fallback = mock(AuthenticationSessionProvider.class);
        when(session.getProvider(AuthenticationSessionProvider.class)).thenReturn(fallback);

        provider.authSessions();
        provider.authSessions();

        verify(session, times(1)).getProvider(eq(AuthenticationSessionProvider.class),
                eq(ValkeyDatastoreProviderFactory.PROVIDER_ID));
        verify(session, times(1)).getProvider(AuthenticationSessionProvider.class);
        verifyNoMoreInteractions(session);
    }

    @Test
    void clearsOverridesOnClose() {
        AuthenticationSessionProvider valkeyProvider = mock(AuthenticationSessionProvider.class);
        when(session.getProvider(eq(AuthenticationSessionProvider.class),
                eq(ValkeyDatastoreProviderFactory.PROVIDER_ID))).thenReturn(valkeyProvider);

        provider.authSessions();
        provider.close();

        AuthenticationSessionProvider fallback = mock(AuthenticationSessionProvider.class);
        when(session.getProvider(eq(AuthenticationSessionProvider.class),
                eq(ValkeyDatastoreProviderFactory.PROVIDER_ID))).thenReturn(null);
        when(session.getProvider(AuthenticationSessionProvider.class)).thenReturn(fallback);

        assertSame(fallback, provider.authSessions());
        verify(session, times(2)).getProvider(eq(AuthenticationSessionProvider.class),
                eq(ValkeyDatastoreProviderFactory.PROVIDER_ID));
        verify(session, times(1)).getProvider(AuthenticationSessionProvider.class);
        verifyNoMoreInteractions(session);
    }

    @Test
    void prefersValkeyForCoreProviders() {
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
        when(session.getProvider(eq(IdentityProviderStorageProvider.class),
                eq(ValkeyDatastoreProviderFactory.PROVIDER_ID))).thenReturn(idpProvider);
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
