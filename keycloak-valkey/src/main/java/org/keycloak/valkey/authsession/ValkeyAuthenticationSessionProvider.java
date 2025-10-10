package org.keycloak.valkey.authsession;

import java.util.Map;
import java.util.Objects;

import org.keycloak.common.util.Time;
import org.keycloak.models.ClientModel;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.sessions.AuthenticationSessionCompoundId;
import org.keycloak.sessions.AuthenticationSessionProvider;
import org.keycloak.sessions.RootAuthenticationSessionModel;
import org.keycloak.valkey.ValkeyConnectionProvider;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

/**
 * {@link AuthenticationSessionProvider} backed by Valkey.
 */
final class ValkeyAuthenticationSessionProvider implements AuthenticationSessionProvider {

    private static final String KEY_PREFIX = "auth-session";

    private final KeycloakSession session;
    private final ValkeyAuthenticationSessionStore store;
    private final int authSessionsLimit;

    ValkeyAuthenticationSessionProvider(KeycloakSession session, ValkeyConnectionProvider connectionProvider,
            int authSessionsLimit) {
        this.session = Objects.requireNonNull(session, "session");
        ValkeyConnectionProvider provider = Objects.requireNonNull(connectionProvider, "connectionProvider");
        StatefulRedisConnection<String, String> connection = provider.getConnection();
        RedisCommands<String, String> commands = connection.sync();
        this.store = new ValkeyAuthenticationSessionStore(commands, KEY_PREFIX);
        this.authSessionsLimit = authSessionsLimit;
    }

    @Override
    public RootAuthenticationSessionModel createRootAuthenticationSession(RealmModel realm) {
        return createRootAuthenticationSession(realm, org.keycloak.models.utils.KeycloakModelUtils.generateId());
    }

    @Override
    public RootAuthenticationSessionModel createRootAuthenticationSession(RealmModel realm, String id) {
        Objects.requireNonNull(realm, "realm");
        String rootId = (id == null || id.isBlank()) ? org.keycloak.models.utils.KeycloakModelUtils.generateId() : id;
        int now = Time.currentTime();
        ValkeyRootAuthenticationSessionEntity entity = new ValkeyRootAuthenticationSessionEntity(rootId, realm.getId(), now);
        ValkeyRootAuthenticationSessionEntity persisted = store.create(realm, entity);
        if (persisted == null) {
            throw new IllegalStateException("Failed to persist Valkey authentication session");
        }
        return wrap(realm, persisted);
    }

    @Override
    public RootAuthenticationSessionModel getRootAuthenticationSession(RealmModel realm, String authenticationSessionId) {
        if (realm == null || authenticationSessionId == null) {
            return null;
        }
        return store.load(realm.getId(), authenticationSessionId)
                .map(entity -> wrap(realm, entity))
                .orElse(null);
    }

    @Override
    public void removeRootAuthenticationSession(RealmModel realm, RootAuthenticationSessionModel authenticationSession) {
        if (realm == null || authenticationSession == null) {
            return;
        }
        store.delete(realm.getId(), authenticationSession.getId());
    }

    @Override
    public void removeAllExpired() {
        // TTL is enforced by Valkey.
    }

    @Override
    public void removeExpired(RealmModel realm) {
        // TTL is enforced by Valkey.
    }

    @Override
    public void onRealmRemoved(RealmModel realm) {
        if (realm == null) {
            return;
        }
        store.deleteAllForRealm(realm.getId());
    }

    @Override
    public void onClientRemoved(RealmModel realm, ClientModel client) {
        // Client removal is handled lazily when sessions are accessed.
    }

    @Override
    public void updateNonlocalSessionAuthNotes(AuthenticationSessionCompoundId compoundId, Map<String, String> authNotesFragment) {
        if (compoundId == null || authNotesFragment == null || authNotesFragment.isEmpty()) {
            return;
        }
        store.loadById(compoundId.getRootSessionId()).ifPresent(entity -> {
            RealmModel realm = session.realms().getRealm(entity.getRealmId());
            if (realm != null) {
                store.updateAuthNotes(realm, entity.getId(), compoundId.getTabId(), authNotesFragment);
            }
        });
    }

    @Override
    public void close() {
        // Connection lifecycle is handled by the Valkey connection provider.
    }

    ValkeyRootAuthenticationSessionAdapter wrap(RealmModel realm, ValkeyRootAuthenticationSessionEntity entity) {
        return new ValkeyRootAuthenticationSessionAdapter(session, this, realm, entity, authSessionsLimit);
    }

    ValkeyRootAuthenticationSessionEntity update(RealmModel realm, String id,
            java.util.function.UnaryOperator<ValkeyRootAuthenticationSessionEntity> updater) {
        return store.update(realm, id, updater);
    }

    ValkeyRootAuthenticationSessionEntity reload(RealmModel realm, String id) {
        return store.load(realm.getId(), id).orElse(null);
    }
}
