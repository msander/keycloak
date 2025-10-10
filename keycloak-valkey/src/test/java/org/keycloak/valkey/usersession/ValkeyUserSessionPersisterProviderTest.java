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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.keycloak.common.util.Time;
import org.keycloak.common.Profile;
import org.keycloak.models.AuthenticatedClientSessionModel;
import org.keycloak.models.ClientModel;
import org.keycloak.models.KeycloakContext;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.models.RealmProvider;
import org.keycloak.models.UserModel;
import org.keycloak.models.UserProvider;
import org.keycloak.models.UserSessionModel;
import org.keycloak.models.session.UserSessionPersisterProvider;
import org.keycloak.valkey.ValkeyConnectionProvider;
import org.keycloak.valkey.connection.LettuceValkeyConnectionProviderFactory;
import org.keycloak.valkey.testing.EmbeddedValkeyServer;
import org.keycloak.valkey.testing.MapBackedConfigScope;

class ValkeyUserSessionPersisterProviderTest {

    private static EmbeddedValkeyServer server;
    private static LettuceValkeyConnectionProviderFactory connectionFactory;

    private KeycloakSession session;
    private ValkeyConnectionProvider connectionProvider;
    private RealmModel realm;
    private UserModel user;
    private ClientModel client;
    private ValkeyUserSessionProvider sessionProvider;
    private UserSessionPersisterProvider persister;

    @BeforeAll
    static void startServer() {
        Profile.init(Profile.ProfileName.DEFAULT,
                Arrays.stream(Profile.Feature.values()).collect(Collectors.toMap(feature -> feature, feature -> Boolean.TRUE)));
        server = EmbeddedValkeyServer.builder()
                .startupTimeout(Duration.ofSeconds(5))
                .start();
        connectionFactory = new LettuceValkeyConnectionProviderFactory();
        Map<String, String> config = Map.of("uri", "redis://" + server.getHost() + ':' + server.getPort());
        connectionFactory.init(MapBackedConfigScope.from(config));
    }

    @AfterAll
    static void shutdown() {
        Profile.reset();
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
        connectionProvider.getConnection().sync().flushall();

        KeycloakContext context = mock(KeycloakContext.class);
        when(session.getContext()).thenReturn(context);
        doNothing().when(context).setAuthenticationSession(any());

        RealmProvider realmProvider = mock(RealmProvider.class);
        when(session.realms()).thenReturn(realmProvider);
        realm = mock(RealmModel.class);
        when(realm.getId()).thenReturn("realm-1");
        when(realm.getName()).thenReturn("realm-1");
        when(realm.getSsoSessionMaxLifespan()).thenReturn(1800);
        when(realm.getSsoSessionMaxLifespanRememberMe()).thenReturn(1800);
        when(realm.getSsoSessionIdleTimeout()).thenReturn(900);
        when(realm.getSsoSessionIdleTimeoutRememberMe()).thenReturn(900);
        when(realm.isOfflineSessionMaxLifespanEnabled()).thenReturn(false);
        when(realm.getOfflineSessionMaxLifespan()).thenReturn(0);
        when(realm.getOfflineSessionIdleTimeout()).thenReturn(43200);
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

        sessionProvider = new ValkeyUserSessionProvider(session, connectionProvider);
        persister = new ValkeyUserSessionPersisterProvider(session, connectionProvider);
    }

    @AfterEach
    void tearDown() {
        Time.setOffset(0);
        if (persister != null) {
            persister.close();
        }
        if (sessionProvider != null) {
            sessionProvider.close();
        }
        if (connectionProvider != null) {
            connectionProvider.close();
        }
    }

    @Test
    void shouldPersistAndLoadUserSession() {
        UserSessionModel created = sessionProvider.createUserSession(null, realm, user, "alice", "127.0.0.1", "form",
                false, null, null, UserSessionModel.SessionPersistenceState.PERSISTENT);
        persister.createUserSession(created, false);

        UserSessionModel loaded = persister.loadUserSession(realm, created.getId(), false);
        assertNotNull(loaded);
        assertEquals(created.getId(), loaded.getId());
        assertEquals(user.getId(), loaded.getUser().getId());
    }

    @Test
    void shouldPersistClientSessionsAndProvideCounts() {
        UserSessionModel userSession = sessionProvider.createUserSession(null, realm, user, "alice", "127.0.0.1", "form",
                false, null, null, UserSessionModel.SessionPersistenceState.PERSISTENT);
        AuthenticatedClientSessionModel clientSession = sessionProvider.createClientSession(realm, client, userSession);

        persister.createUserSession(userSession, false);
        persister.createClientSession(clientSession, false);

        assertEquals(1, persister.getUserSessionsCount(false));
        assertEquals(1, persister.getUserSessionsCount(realm, client, false));
        Map<String, Long> counts = persister.getUserSessionsCountsByClients(realm, false);
        assertEquals(1L, counts.get(client.getId()));

        AuthenticatedClientSessionModel loaded = persister.loadClientSession(realm, client, userSession, false);
        assertNotNull(loaded);
        assertEquals(clientSession.getClient().getId(), loaded.getClient().getId());
    }

    @Test
    void shouldHandleOfflineSessions() {
        UserSessionModel online = sessionProvider.createUserSession(null, realm, user, "alice", "127.0.0.1", "form",
                false, null, null, UserSessionModel.SessionPersistenceState.PERSISTENT);
        UserSessionModel offline = sessionProvider.createOfflineUserSession(online);

        persister.createUserSession(offline, true);
        UserSessionModel loaded = persister.loadUserSession(realm, offline.getId(), true);
        assertNotNull(loaded);
        assertTrue(loaded.isOffline());
    }

    @Test
    void shouldRemoveExpiredSessions() {
        when(realm.getSsoSessionMaxLifespan()).thenReturn(10);
        when(realm.getSsoSessionIdleTimeout()).thenReturn(5);

        UserSessionModel sessionModel = sessionProvider.createUserSession(null, realm, user, "alice", "127.0.0.1", "form",
                false, null, null, UserSessionModel.SessionPersistenceState.PERSISTENT);
        persister.createUserSession(sessionModel, false);

        persister.updateLastSessionRefreshes(realm, 1, List.of(sessionModel.getId()), false);
        Time.setOffset(3600);

        persister.removeExpired(realm);
        assertNull(persister.loadUserSession(realm, sessionModel.getId(), false));
    }
}
