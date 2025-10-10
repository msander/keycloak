package org.keycloak.valkey.authsession;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.keycloak.models.ClientModel;
import org.keycloak.models.KeycloakContext;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.models.RealmModel;
import org.keycloak.models.RealmProvider;
import org.keycloak.sessions.AuthenticationSessionCompoundId;
import org.keycloak.sessions.AuthenticationSessionModel;
import org.keycloak.sessions.RootAuthenticationSessionModel;
import org.keycloak.valkey.ValkeyConnectionProvider;
import org.keycloak.valkey.connection.LettuceValkeyConnectionProviderFactory;
import org.keycloak.valkey.testing.EmbeddedValkeyServer;
import org.keycloak.valkey.testing.MapBackedConfigScope;

class ValkeyAuthenticationSessionProviderTest {

    private static EmbeddedValkeyServer server;
    private static LettuceValkeyConnectionProviderFactory connectionFactory;

    private KeycloakSession session;
    private ValkeyConnectionProvider connectionProvider;
    private RealmModel realm;
    private ClientModel client;
    private ValkeyAuthenticationSessionProvider provider;

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
        connectionProvider = connectionFactory.create(mock(KeycloakSession.class));
        session = mock(KeycloakSession.class);
        KeycloakContext context = mock(KeycloakContext.class);
        when(session.getContext()).thenReturn(context);
        doNothing().when(context).setAuthenticationSession(any(AuthenticationSessionModel.class));
        when(session.getProvider(eq(ValkeyConnectionProvider.class))).thenReturn(connectionProvider);

        RealmProvider realmProvider = mock(RealmProvider.class);
        when(session.realms()).thenReturn(realmProvider);

        realm = mock(RealmModel.class);
        when(realm.getId()).thenReturn("realm-1");
        when(realmProvider.getRealm(eq(realm.getId()))).thenReturn(realm);

        client = mock(ClientModel.class);
        when(client.getId()).thenReturn("client-1");
        when(realm.getClientById(eq(client.getId()))).thenReturn(client);

        provider = new ValkeyAuthenticationSessionProvider(session, connectionProvider, 5);
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
    void shouldCreateAndRetrieveRootSession() {
        RootAuthenticationSessionModel root = provider.createRootAuthenticationSession(realm);

        assertNotNull(root);
        assertNotNull(root.getId());

        RootAuthenticationSessionModel reloaded = provider.getRootAuthenticationSession(realm, root.getId());
        assertNotNull(reloaded);
        assertEquals(root.getId(), reloaded.getId());
    }

    @Test
    void shouldCreateAuthenticationSessionForClient() {
        RootAuthenticationSessionModel root = provider.createRootAuthenticationSession(realm);
        AuthenticationSessionModel authSession = root.createAuthenticationSession(client);

        assertNotNull(authSession);
        assertEquals(client, authSession.getClient());

        RootAuthenticationSessionModel reloaded = provider.getRootAuthenticationSession(realm, root.getId());
        assertTrue(reloaded.getAuthenticationSessions().containsKey(authSession.getTabId()));
    }

    @Test
    void shouldPersistAuthNotesUpdates() {
        RootAuthenticationSessionModel root = provider.createRootAuthenticationSession(realm);
        AuthenticationSessionModel authSession = root.createAuthenticationSession(client);

        authSession.setAuthNote("otp", "123456");

        RootAuthenticationSessionModel reloaded = provider.getRootAuthenticationSession(realm, root.getId());
        AuthenticationSessionModel persisted = reloaded.getAuthenticationSessions().get(authSession.getTabId());
        assertEquals("123456", persisted.getAuthNote("otp"));
    }

    @Test
    void removingLastTabShouldRemoveRootSession() {
        RootAuthenticationSessionModel root = provider.createRootAuthenticationSession(realm);
        AuthenticationSessionModel authSession = root.createAuthenticationSession(client);

        root.removeAuthenticationSessionByTabId(authSession.getTabId());

        RootAuthenticationSessionModel reloaded = provider.getRootAuthenticationSession(realm, root.getId());
        assertNull(reloaded);
    }

    @Test
    void updateNonlocalNotesShouldWriteToStore() {
        RootAuthenticationSessionModel root = provider.createRootAuthenticationSession(realm);
        AuthenticationSessionModel authSession = root.createAuthenticationSession(client);
        AuthenticationSessionCompoundId compoundId = AuthenticationSessionCompoundId
                .decoded(root.getId(), authSession.getTabId(), client.getId());

        provider.updateNonlocalSessionAuthNotes(compoundId, Map.of("device", "trusted"));

        RootAuthenticationSessionModel reloaded = provider.getRootAuthenticationSession(realm, root.getId());
        assertEquals("trusted", reloaded.getAuthenticationSessions().get(authSession.getTabId()).getAuthNote("device"));
    }
}
