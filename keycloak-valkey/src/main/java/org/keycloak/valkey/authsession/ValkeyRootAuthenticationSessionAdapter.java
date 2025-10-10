package org.keycloak.valkey.authsession;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.UnaryOperator;

import org.keycloak.common.util.Base64Url;
import org.keycloak.common.util.SecretGenerator;
import org.keycloak.common.util.Time;
import org.keycloak.models.ClientModel;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.sessions.AuthenticationSessionModel;
import org.keycloak.sessions.RootAuthenticationSessionModel;

final class ValkeyRootAuthenticationSessionAdapter implements RootAuthenticationSessionModel {

    private static final Comparator<Map.Entry<String, ValkeyAuthenticationSessionEntity>> TIMESTAMP_COMPARATOR =
            Comparator.comparingInt(entry -> entry.getValue().getTimestamp());

    private final KeycloakSession session;
    private final ValkeyAuthenticationSessionProvider provider;
    private final RealmModel realm;
    private final int authSessionsLimit;

    private ValkeyRootAuthenticationSessionEntity entity;

    ValkeyRootAuthenticationSessionAdapter(KeycloakSession session, ValkeyAuthenticationSessionProvider provider,
            RealmModel realm, ValkeyRootAuthenticationSessionEntity entity, int authSessionsLimit) {
        this.session = Objects.requireNonNull(session, "session");
        this.provider = Objects.requireNonNull(provider, "provider");
        this.realm = Objects.requireNonNull(realm, "realm");
        this.entity = Objects.requireNonNull(entity, "entity");
        this.authSessionsLimit = authSessionsLimit;
    }

    @Override
    public String getId() {
        return entity.getId();
    }

    @Override
    public RealmModel getRealm() {
        return realm;
    }

    @Override
    public int getTimestamp() {
        return entity.getTimestamp();
    }

    @Override
    public void setTimestamp(int timestamp) {
        mutate(root -> {
            root.setTimestamp(timestamp);
            return root;
        });
    }

    @Override
    public Map<String, AuthenticationSessionModel> getAuthenticationSessions() {
        refresh();
        Map<String, AuthenticationSessionModel> result = new LinkedHashMap<>();
        entity.getAuthenticationSessions().forEach((tabId, authEntity) -> result.put(tabId,
                new ValkeyAuthenticationSessionAdapter(session, this, tabId, authEntity.copy())));
        return result;
    }

    @Override
    public AuthenticationSessionModel getAuthenticationSession(ClientModel client, String tabId) {
        if (client == null || tabId == null) {
            return null;
        }
        AuthenticationSessionModel model = getAuthenticationSessions().get(tabId);
        if (model != null && client.equals(model.getClient())) {
            session.getContext().setAuthenticationSession(model);
            return model;
        }
        return null;
    }

    @Override
    public AuthenticationSessionModel createAuthenticationSession(ClientModel client) {
        Objects.requireNonNull(client, "client");
        String tabId = Base64Url.encode(SecretGenerator.getInstance().randomBytes(8));
        int now = Time.currentTime();
        ValkeyAuthenticationSessionEntity newEntity = ValkeyAuthenticationSessionEntity.forClient(client.getId(), now);
        mutate(root -> {
            Map<String, ValkeyAuthenticationSessionEntity> sessions = new LinkedHashMap<>(root.getAuthenticationSessions());
            if (sessions.size() >= authSessionsLimit && !sessions.containsKey(tabId)) {
                Optional<String> oldest = sessions.entrySet().stream().min(TIMESTAMP_COMPARATOR).map(Map.Entry::getKey);
                oldest.ifPresent(sessions::remove);
            }
            newEntity.setTimestamp(now);
            sessions.put(tabId, newEntity);
            root.setAuthenticationSessions(sessions);
            root.setTimestamp(now);
            return sessions.isEmpty() ? null : root;
        });
        AuthenticationSessionModel model = new ValkeyAuthenticationSessionAdapter(session, this, tabId, newEntity.copy());
        session.getContext().setAuthenticationSession(model);
        return model;
    }

    @Override
    public void removeAuthenticationSessionByTabId(String tabId) {
        mutate(root -> {
            Map<String, ValkeyAuthenticationSessionEntity> sessions = new LinkedHashMap<>(root.getAuthenticationSessions());
            if (sessions.remove(tabId) == null) {
                return root;
            }
            if (sessions.isEmpty()) {
                return null;
            }
            root.setAuthenticationSessions(sessions);
            root.setTimestamp(Time.currentTime());
            return root;
        });
    }

    @Override
    public void restartSession(RealmModel realm) {
        mutate(root -> {
            root.getAuthenticationSessions().clear();
            root.setTimestamp(Time.currentTime());
            return root;
        });
    }

    ValkeyAuthenticationSessionEntity updateAuthenticationSession(String tabId,
            UnaryOperator<ValkeyAuthenticationSessionEntity> mutator) {
        ValkeyAuthenticationSessionEntity[] updatedHolder = new ValkeyAuthenticationSessionEntity[1];
        mutate(root -> {
            Map<String, ValkeyAuthenticationSessionEntity> sessions = new LinkedHashMap<>(root.getAuthenticationSessions());
            ValkeyAuthenticationSessionEntity existing = sessions.get(tabId);
            if (existing == null) {
                return root;
            }
            ValkeyAuthenticationSessionEntity updated = mutator.apply(existing.copy());
            if (updated == null) {
                sessions.remove(tabId);
            } else {
                updated.setTimestamp(Time.currentTime());
                sessions.put(tabId, updated);
                updatedHolder[0] = updated;
            }
            if (sessions.isEmpty()) {
                return null;
            }
            root.setAuthenticationSessions(sessions);
            root.setTimestamp(Time.currentTime());
            return root;
        });
        return updatedHolder[0];
    }

    void refresh() {
        ValkeyRootAuthenticationSessionEntity latest = provider.reload(realm, entity.getId());
        if (latest != null) {
            entity = latest;
        }
    }

    private void mutate(UnaryOperator<ValkeyRootAuthenticationSessionEntity> mutator) {
        ValkeyRootAuthenticationSessionEntity updated = provider.update(realm, entity.getId(), current -> {
            if (current == null) {
                return null;
            }
            ValkeyRootAuthenticationSessionEntity copy = current.copy();
            ValkeyRootAuthenticationSessionEntity result = mutator.apply(copy);
            return result;
        });
        if (updated == null) {
            entity = new ValkeyRootAuthenticationSessionEntity(entity.getId(), realm.getId(), Time.currentTime());
            entity.getAuthenticationSessions().clear();
        } else {
            entity = updated;
        }
    }
}
