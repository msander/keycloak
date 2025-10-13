package org.keycloak.valkey.usersession;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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
import org.keycloak.models.AuthenticatedClientSessionModel;
import org.keycloak.models.ClientModel;
import org.keycloak.models.KeycloakContext;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.models.RealmProvider;
import org.keycloak.models.UserModel;
import org.keycloak.models.UserProvider;
import org.keycloak.models.UserSessionModel;
import org.keycloak.valkey.ValkeyConnectionProvider;
import org.keycloak.valkey.connection.LettuceValkeyConnectionProviderFactory;
import org.keycloak.valkey.testing.EmbeddedValkeyServer;
import org.keycloak.valkey.testing.MapBackedConfigScope;

class ValkeyUserSessionProviderTest {

    private static EmbeddedValkeyServer server;
    private static LettuceValkeyConnectionProviderFactory connectionFactory;

    private KeycloakSession session;
    private ValkeyConnectionProvider connectionProvider;
    private RealmModel realm;
    private UserModel user;
    private ClientModel client;
    private ValkeyUserSessionProvider provider;

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
        session = mock(KeycloakSession.class);
        connectionProvider = connectionFactory.create(session);
        when(session.getProvider(eq(ValkeyConnectionProvider.class))).thenReturn(connectionProvider);

        KeycloakContext context = mock(KeycloakContext.class);
        when(session.getContext()).thenReturn(context);
        doNothing().when(context).setAuthenticationSession(any());

        RealmProvider realmProvider = mock(RealmProvider.class);
        when(session.realms()).thenReturn(realmProvider);
        realm = mock(RealmModel.class);
        when(realm.getId()).thenReturn("realm-1");
        when(realm.getSsoSessionMaxLifespan()).thenReturn(3600);
        when(realm.getSsoSessionMaxLifespanRememberMe()).thenReturn(3600);
        when(realm.getSsoSessionIdleTimeout()).thenReturn(1800);
        when(realm.getSsoSessionIdleTimeoutRememberMe()).thenReturn(1800);
        when(realm.isOfflineSessionMaxLifespanEnabled()).thenReturn(false);
        when(realm.getOfflineSessionMaxLifespan()).thenReturn(0);
        when(realm.getOfflineSessionIdleTimeout()).thenReturn(43200);
        when(realm.getClientSessionMaxLifespan()).thenReturn(0);
        when(realm.getClientSessionIdleTimeout()).thenReturn(0);
        when(realm.getClientOfflineSessionMaxLifespan()).thenReturn(0);
        when(realm.getClientOfflineSessionIdleTimeout()).thenReturn(0);
        when(realmProvider.getRealm(realm.getId())).thenReturn(realm);

        client = mock(ClientModel.class);
        when(client.getId()).thenReturn("client-1");
        when(client.getProtocol()).thenReturn("openid-connect");
        when(realm.getClientById(client.getId())).thenReturn(client);

        UserProvider userProvider = mock(UserProvider.class);
        when(session.users()).thenReturn(userProvider);
        user = mock(UserModel.class);
        when(user.getId()).thenReturn("user-1");
        when(userProvider.getUserById(realm, user.getId())).thenReturn(user);

        provider = new ValkeyUserSessionProvider(session, connectionProvider);
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
    void shouldCreateAndRetrieveUserSession() {
        UserSessionModel sessionModel = provider.createUserSession(null, realm, user, "alice", "127.0.0.1", "form",
                false, null, null, UserSessionModel.SessionPersistenceState.PERSISTENT);

        assertNotNull(sessionModel);
        assertNotNull(sessionModel.getId());

        UserSessionModel reloaded = provider.getUserSession(realm, sessionModel.getId());
        assertNotNull(reloaded);
        assertEquals(sessionModel.getId(), reloaded.getId());
    }

    @Test
    void shouldAttachClientSessionToUserSession() {
        UserSessionModel sessionModel = provider.createUserSession(null, realm, user, "alice", "127.0.0.1", "form",
                false, null, null, UserSessionModel.SessionPersistenceState.PERSISTENT);

        AuthenticatedClientSessionModel clientSession = provider.createClientSession(realm, client, sessionModel);
        assertNotNull(clientSession);

        UserSessionModel reloaded = provider.getUserSession(realm, sessionModel.getId());
        assertNotNull(reloaded.getAuthenticatedClientSessionByClient(client.getId()));
    }

    @Test
    void shouldCreateOfflineUserAndClientSessions() {
        UserSessionModel online = provider.createUserSession(null, realm, user, "alice", "127.0.0.1", "form",
                false, null, null, UserSessionModel.SessionPersistenceState.PERSISTENT);
        UserSessionModel offline = provider.createOfflineUserSession(online);
        assertTrue(offline.isOffline());

        AuthenticatedClientSessionModel offlineClient = provider.createOfflineClientSession(
                provider.createClientSession(realm, client, online), offline);
        assertNotNull(offlineClient);

        UserSessionModel reloadedOffline = provider.getOfflineUserSession(realm, offline.getId());
        assertNotNull(reloadedOffline.getAuthenticatedClientSessionByClient(client.getId()));
    }

    @Test
    void shouldRemoveSessionsForUser() {
        UserSessionModel sessionOne = provider.createUserSession(null, realm, user, "alice", "127.0.0.1", "form",
                false, null, null, UserSessionModel.SessionPersistenceState.PERSISTENT);
        UserSessionModel sessionTwo = provider.createUserSession(null, realm, user, "alice", "127.0.0.1", "form",
                false, null, null, UserSessionModel.SessionPersistenceState.PERSISTENT);

        provider.removeUserSessions(realm, user);

        assertNull(provider.getUserSession(realm, sessionOne.getId()));
        assertNull(provider.getUserSession(realm, sessionTwo.getId()));
    }

    @Test
    void shouldClearSessionsWhenRealmRemoved() {
        UserSessionModel sessionModel = provider.createUserSession(null, realm, user, "alice", "127.0.0.1", "form",
                false, null, null, UserSessionModel.SessionPersistenceState.PERSISTENT);
        provider.removeUserSessions(realm);
        assertNull(provider.getUserSession(realm, sessionModel.getId()));
    }

    @Test
    void shouldExposeStableClientSessionId() {
        UserSessionModel sessionModel = provider.createUserSession(null, realm, user, "alice", "127.0.0.1", "form",
                false, null, null, UserSessionModel.SessionPersistenceState.PERSISTENT);

        AuthenticatedClientSessionModel clientSession = provider.createClientSession(realm, client, sessionModel);

        String expectedId = sessionModel.getId() + "::" + client.getId();
        assertEquals(expectedId, clientSession.getId());

        AuthenticatedClientSessionModel reloaded = provider.getClientSession(sessionModel, client, false);
        assertNotNull(reloaded);
        assertEquals(expectedId, reloaded.getId());
    }
}
