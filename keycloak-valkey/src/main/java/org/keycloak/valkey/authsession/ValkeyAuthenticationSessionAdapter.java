package org.keycloak.valkey.authsession;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.keycloak.common.Profile;
import org.keycloak.common.Profile.Feature;
import org.keycloak.models.ClientModel;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.models.UserModel;
import org.keycloak.models.light.LightweightUserAdapter;
import org.keycloak.sessions.AuthenticationSessionModel;
import org.keycloak.sessions.RootAuthenticationSessionModel;

import static org.keycloak.models.Constants.SESSION_NOTE_LIGHTWEIGHT_USER;
import static org.keycloak.models.light.LightweightUserAdapter.isLightweightUser;

final class ValkeyAuthenticationSessionAdapter implements AuthenticationSessionModel {

    private final KeycloakSession session;
    private final ValkeyRootAuthenticationSessionAdapter parent;
    private final String tabId;

    private ValkeyAuthenticationSessionEntity entity;

    ValkeyAuthenticationSessionAdapter(KeycloakSession session, ValkeyRootAuthenticationSessionAdapter parent, String tabId,
            ValkeyAuthenticationSessionEntity entity) {
        this.session = Objects.requireNonNull(session, "session");
        this.parent = Objects.requireNonNull(parent, "parent");
        this.tabId = Objects.requireNonNull(tabId, "tabId");
        this.entity = Objects.requireNonNull(entity, "entity");
    }

    @Override
    public String getTabId() {
        return tabId;
    }

    @Override
    public RootAuthenticationSessionModel getParentSession() {
        return parent;
    }

    @Override
    public RealmModel getRealm() {
        return parent.getRealm();
    }

    @Override
    public ClientModel getClient() {
        return getRealm().getClientById(entity.getClientUUID());
    }

    @Override
    public String getRedirectUri() {
        return entity.getRedirectUri();
    }

    @Override
    public void setRedirectUri(String uri) {
        update(e -> e.setRedirectUri(uri));
    }

    @Override
    public String getAction() {
        return entity.getAction();
    }

    @Override
    public void setAction(String action) {
        update(e -> e.setAction(action));
    }

    @Override
    public Set<String> getClientScopes() {
        if (entity.getClientScopes() == null || entity.getClientScopes().isEmpty()) {
            return Collections.emptySet();
        }
        return new HashSet<>(entity.getClientScopes());
    }

    @Override
    public void setClientScopes(Set<String> clientScopes) {
        update(e -> e.setClientScopes(clientScopes));
    }

    @Override
    public String getProtocol() {
        return entity.getProtocol();
    }

    @Override
    public void setProtocol(String protocol) {
        update(e -> e.setProtocol(protocol));
    }

    @Override
    public String getClientNote(String name) {
        return (entity.getClientNotes() != null && name != null) ? entity.getClientNotes().get(name) : null;
    }

    @Override
    public void setClientNote(String name, String value) {
        update(e -> {
            Map<String, String> notes = e.getClientNotes();
            if (notes == null) {
                notes = new ConcurrentHashMap<>();
                e.setClientNotes(notes);
            }
            if (name != null) {
                if (value == null) {
                    notes.remove(name);
                } else {
                    notes.put(name, value);
                }
            }
        });
    }

    @Override
    public void removeClientNote(String name) {
        if (entity.getClientNotes() == null || name == null) {
            return;
        }
        update(e -> e.getClientNotes().remove(name));
    }

    @Override
    public Map<String, String> getClientNotes() {
        if (entity.getClientNotes() == null || entity.getClientNotes().isEmpty()) {
            return Collections.emptyMap();
        }
        return new ConcurrentHashMap<>(entity.getClientNotes());
    }

    @Override
    public void clearClientNotes() {
        update(e -> e.setClientNotes(new ConcurrentHashMap<>()));
    }

    @Override
    public String getAuthNote(String name) {
        return (entity.getAuthNotes() != null && name != null) ? entity.getAuthNotes().get(name) : null;
    }

    @Override
    public void setAuthNote(String name, String value) {
        update(e -> {
            Map<String, String> notes = e.getAuthNotes();
            if (notes == null) {
                notes = new ConcurrentHashMap<>();
                e.setAuthNotes(notes);
            }
            if (name != null) {
                if (value == null) {
                    notes.remove(name);
                } else {
                    notes.put(name, value);
                }
            }
        });
    }

    @Override
    public void removeAuthNote(String name) {
        if (entity.getAuthNotes() == null || name == null) {
            return;
        }
        update(e -> e.getAuthNotes().remove(name));
    }

    @Override
    public void clearAuthNotes() {
        update(e -> e.setAuthNotes(new ConcurrentHashMap<>()));
    }

    @Override
    public void setUserSessionNote(String name, String value) {
        update(e -> {
            Map<String, String> notes = e.getUserSessionNotes();
            if (notes == null) {
                notes = new ConcurrentHashMap<>();
                e.setUserSessionNotes(notes);
            }
            if (name != null) {
                if (value == null) {
                    notes.remove(name);
                } else {
                    notes.put(name, value);
                }
            }
        });
    }

    @Override
    public Map<String, String> getUserSessionNotes() {
        if (entity.getUserSessionNotes() == null || entity.getUserSessionNotes().isEmpty()) {
            return Collections.emptyMap();
        }
        return new ConcurrentHashMap<>(entity.getUserSessionNotes());
    }

    @Override
    public void clearUserSessionNotes() {
        update(e -> e.setUserSessionNotes(new ConcurrentHashMap<>()));
    }

    @Override
    public Set<String> getRequiredActions() {
        return new HashSet<>(entity.getRequiredActions());
    }

    @Override
    public void addRequiredAction(String action) {
        if (action == null) {
            return;
        }
        update(e -> e.getRequiredActions().add(action));
    }

    @Override
    public void removeRequiredAction(String action) {
        if (action == null) {
            return;
        }
        update(e -> e.getRequiredActions().remove(action));
    }

    @Override
    public void addRequiredAction(UserModel.RequiredAction action) {
        if (action != null) {
            addRequiredAction(action.name());
        }
    }

    @Override
    public void removeRequiredAction(UserModel.RequiredAction action) {
        if (action != null) {
            removeRequiredAction(action.name());
        }
    }

    @Override
    public Map<String, AuthenticationSessionModel.ExecutionStatus> getExecutionStatus() {
        return entity.getExecutionStatus();
    }

    @Override
    public void setExecutionStatus(String authenticator, AuthenticationSessionModel.ExecutionStatus status) {
        if (authenticator == null || status == null) {
            return;
        }
        update(e -> e.getExecutionStatus().put(authenticator, status));
    }

    @Override
    public void clearExecutionStatus() {
        update(e -> e.getExecutionStatus().clear());
    }

    @Override
    public UserModel getAuthenticatedUser() {
        if (entity.getAuthUserId() == null) {
            return null;
        }
        if (Profile.isFeatureEnabled(Feature.TRANSIENT_USERS)
                && getUserSessionNotes().containsKey(SESSION_NOTE_LIGHTWEIGHT_USER)) {
            LightweightUserAdapter cached = session.getAttribute("authSession.user." + parent.getId(),
                    LightweightUserAdapter.class);
            if (cached != null) {
                return cached;
            }
            LightweightUserAdapter adapter = LightweightUserAdapter.fromString(session, getRealm(),
                    getUserSessionNotes().get(SESSION_NOTE_LIGHTWEIGHT_USER));
            session.setAttribute("authSession.user." + parent.getId(), adapter);
            adapter.setUpdateHandler(updated -> {
                if (adapter == updated) {
                    setUserSessionNote(SESSION_NOTE_LIGHTWEIGHT_USER, updated.serialize());
                }
            });
            return adapter;
        }
        return session.users().getUserById(getRealm(), entity.getAuthUserId());
    }

    @Override
    public void setAuthenticatedUser(UserModel user) {
        update(e -> {
            Map<String, String> notes = e.getUserSessionNotes();
            if (notes == null) {
                notes = new ConcurrentHashMap<>();
                e.setUserSessionNotes(notes);
            }
            if (user == null) {
                e.setAuthUserId(null);
                notes.remove(SESSION_NOTE_LIGHTWEIGHT_USER);
                return;
            }
            e.setAuthUserId(user.getId());
            if (isLightweightUser(user)) {
                LightweightUserAdapter adapter = (LightweightUserAdapter) user;
                notes.put(SESSION_NOTE_LIGHTWEIGHT_USER, adapter.serialize());
                adapter.setUpdateHandler(updated -> {
                    if (adapter == updated) {
                        setUserSessionNote(SESSION_NOTE_LIGHTWEIGHT_USER, updated.serialize());
                    }
                });
            } else {
                notes.remove(SESSION_NOTE_LIGHTWEIGHT_USER);
            }
        });
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof AuthenticationSessionModel that && Objects.equals(that.getTabId(), getTabId()));
    }

    @Override
    public int hashCode() {
        return getTabId().hashCode();
    }

    private void update(Consumer<ValkeyAuthenticationSessionEntity> mutator) {
        ValkeyAuthenticationSessionEntity updated = parent.updateAuthenticationSession(tabId, entity -> {
            mutator.accept(entity);
            return entity;
        });
        if (updated != null) {
            entity = updated;
        }
    }
}
