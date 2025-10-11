package org.keycloak.valkey.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.keycloak.cluster.ClusterEvent;
import org.keycloak.cluster.ClusterProvider;
import org.keycloak.component.ComponentModel;
import org.keycloak.common.util.MultivaluedHashMap;
import org.keycloak.models.cache.infinispan.authorization.events.PermissionTicketRemovedEvent;
import org.keycloak.models.cache.infinispan.authorization.events.PermissionTicketUpdatedEvent;
import org.keycloak.models.cache.infinispan.authorization.events.PolicyRemovedEvent;
import org.keycloak.models.cache.infinispan.authorization.events.PolicyUpdatedEvent;
import org.keycloak.models.cache.infinispan.authorization.events.ResourceRemovedEvent;
import org.keycloak.models.cache.infinispan.authorization.events.ResourceServerRemovedEvent;
import org.keycloak.models.cache.infinispan.authorization.events.ResourceServerUpdatedEvent;
import org.keycloak.models.cache.infinispan.authorization.events.ResourceUpdatedEvent;
import org.keycloak.models.cache.infinispan.authorization.events.ScopeRemovedEvent;
import org.keycloak.models.cache.infinispan.authorization.events.ScopeUpdatedEvent;
import org.keycloak.models.cache.infinispan.events.CacheKeyInvalidatedEvent;
import org.keycloak.models.cache.infinispan.events.ClientAddedEvent;
import org.keycloak.models.cache.infinispan.events.ClientRemovedEvent;
import org.keycloak.models.cache.infinispan.events.ClientScopeAddedEvent;
import org.keycloak.models.cache.infinispan.events.ClientScopeRemovedEvent;
import org.keycloak.models.cache.infinispan.events.ClientUpdatedEvent;
import org.keycloak.models.cache.infinispan.events.GroupAddedEvent;
import org.keycloak.models.cache.infinispan.events.GroupMovedEvent;
import org.keycloak.models.cache.infinispan.events.GroupRemovedEvent;
import org.keycloak.models.cache.infinispan.events.GroupUpdatedEvent;
import org.keycloak.models.cache.infinispan.events.RealmRemovedEvent;
import org.keycloak.models.cache.infinispan.events.RealmUpdatedEvent;
import org.keycloak.models.cache.infinispan.events.RoleAddedEvent;
import org.keycloak.models.cache.infinispan.events.RoleRemovedEvent;
import org.keycloak.models.cache.infinispan.events.RoleUpdatedEvent;
import org.keycloak.models.cache.infinispan.events.UserCacheRealmInvalidationEvent;
import org.keycloak.models.cache.infinispan.events.UserConsentsUpdatedEvent;
import org.keycloak.models.cache.infinispan.events.UserFederationLinkRemovedEvent;
import org.keycloak.models.cache.infinispan.events.UserFederationLinkUpdatedEvent;
import org.keycloak.models.cache.infinispan.events.UserFullInvalidationEvent;
import org.keycloak.models.cache.infinispan.events.UserUpdatedEvent;
import org.keycloak.storage.UserStorageProvider;
import org.keycloak.storage.UserStorageProviderModel;
import org.keycloak.storage.managers.UserStorageSyncManager.UserStorageProviderClusterEvent;

class ValkeyClusterEventCodecTest {

    private final ValkeyClusterEventCodec codec = new ValkeyClusterEventCodec();

    @Test
    void shouldEncodeAndDecodeEvents() {
        ClusterEvent event = new TestClusterEvent("1", "clear");

        byte[] payload = codec.encode("channel", List.of(event), true, ClusterProvider.DCNotify.ALL_DCS, "node-1", "site-a");
        ValkeyClusterEventCodec.DecodedMessage decoded = codec.decode(payload);

        assertEquals("channel", decoded.eventKey());
        assertEquals("node-1", decoded.senderNodeId());
        assertEquals("site-a", decoded.senderSite());
        assertTrue(decoded.ignoreSender());
        assertEquals(1, decoded.events().size());
        assertEquals(event.getClass(), decoded.events().iterator().next().getClass());
        assertTrue(decoded.shouldDeliver("other", "site-a"));
    }

    @Test
    void shouldRespectIgnoreSenderAndSiteFilter() {
        ClusterEvent event = new TestClusterEvent("1", "clear");
        byte[] payload = codec.encode("channel", List.of(event), true, ClusterProvider.DCNotify.LOCAL_DC_ONLY, "node-1", "site-a");
        ValkeyClusterEventCodec.DecodedMessage decoded = codec.decode(payload);

        assertFalse(decoded.shouldDeliver("node-1", "site-a"));
        assertTrue(decoded.shouldDeliver("node-2", "site-a"));
        assertFalse(decoded.shouldDeliver("node-2", "site-b"));
    }

    @Test
    void shouldSerializeWorkCompletionEvents() {
        ValkeyWorkCompletionEvent event = new ValkeyWorkCompletionEvent("task", true);

        byte[] payload = codec.encode("work", List.of(event), true, ClusterProvider.DCNotify.ALL_DCS, "node-1", "site-a");
        ValkeyClusterEventCodec.DecodedMessage decoded = codec.decode(payload);

        assertEquals(1, decoded.events().size());
        assertTrue(decoded.events().iterator().next() instanceof ValkeyWorkCompletionEvent);
    }

    @Test
    void shouldSerializeUserStorageClusterEvents() {
        UserStorageProviderModel provider = sampleUserStorageProvider();
        UserStorageProviderClusterEvent event = UserStorageProviderClusterEvent.createEvent(false, "realm-1", provider);

        byte[] payload = codec.encode("user-storage", List.of(event), true, ClusterProvider.DCNotify.ALL_DCS, "node-1", "site-a");
        ValkeyClusterEventCodec.DecodedMessage decoded = codec.decode(payload);

        assertEquals(1, decoded.events().size());
        Object decodedEventObject = decoded.events().iterator().next();
        assertTrue(decodedEventObject instanceof UserStorageProviderClusterEvent);
        UserStorageProviderClusterEvent decodedEvent = (UserStorageProviderClusterEvent) decodedEventObject;
        assertEquals(event.isRemoved(), decodedEvent.isRemoved());
        assertEquals(event.getRealmId(), decodedEvent.getRealmId());
        assertEquals(provider.getId(), decodedEvent.getStorageProvider().getId());
        assertEquals(provider.getProviderId(), decodedEvent.getStorageProvider().getProviderId());
        assertEquals(provider.getConfig(), decodedEvent.getStorageProvider().getConfig());
    }

    @Test
    void shouldSerializeInvalidationEvents() throws Exception {
        List<ClusterEvent> events = List.of(
                RoleAddedEvent.create("role-add", "realm-a", "analytics"),
                RoleRemovedEvent.create("role-remove", "marketing", "realm-b"),
                RoleUpdatedEvent.create("role-update", "finance", "realm-c"),
                ClientAddedEvent.create("client-add", "realm-x"),
                clientRemovedEvent("client-remove", "realm-y", "client-y",
                        Map.of("role-1", "analyst")),
                ClientUpdatedEvent.create("client-update", "client-z", "realm-z"),
                ClientScopeAddedEvent.create("scope-add", "realm-a"),
                ClientScopeRemovedEvent.create("scope-remove", "realm-b"),
                RealmUpdatedEvent.create("realm-1", "Realm One"),
                RealmRemovedEvent.create("realm-2", "Realm Two"),
                GroupAddedEvent.create("group-add", "parent-1", "realm-c"),
                new GroupRemovedEvent("group-remove", "realm-c", "parent-2"),
                GroupUpdatedEvent.create("group-update"),
                groupMovedEvent("group-moved", "parent-new", "parent-old", "realm-d"),
                new CacheKeyInvalidatedEvent("cache-key"),
                UserUpdatedEvent.create("user-update", "alice", "alice@example.com", "realm-e"),
                userFullInvalidationEvent("user-full", "bob", "bob@example.com", "realm-f", true,
                        Map.of("google", "12345")),
                UserConsentsUpdatedEvent.create("user-consent"),
                UserFederationLinkUpdatedEvent.create("user-fed-update"),
                userFederationLinkRemovedEvent("user-fed-remove", "realm-g", "google", "abc"),
                UserCacheRealmInvalidationEvent.create("realm-h"),
                PermissionTicketUpdatedEvent.create("ticket-update", "owner-a", "requester-a", "resource-a",
                        "Resource A", "scope-a", "server-a"),
                PermissionTicketRemovedEvent.create("ticket-remove", "owner-b", "requester-b", "resource-b",
                        "Resource B", "scope-b", "server-b"),
                PolicyUpdatedEvent.create("policy-update", "Policy A", Set.of("res-1"),
                        Set.of("resource-type"), Set.of("scope-a", "scope-b"), "server-a"),
                PolicyRemovedEvent.create("policy-remove", "Policy B", Set.of("res-2"),
                        Set.of("resource-type"), Set.of("scope-c"), "server-b"),
                ResourceUpdatedEvent.create("resource-update", "Resource A", "type-a", Set.of("/a", "/b"),
                        "owner-a", Set.of("scope-a"), "server-a"),
                ResourceRemovedEvent.create("resource-remove", "Resource B", null, Set.of(),
                        "owner-b", Set.of("scope-b"), "server-b"),
                ResourceServerUpdatedEvent.create("server-update"),
                ResourceServerRemovedEvent.create("server-remove"),
                ScopeUpdatedEvent.create("scope-update", "Scope A", "server-a"),
                ScopeRemovedEvent.create("scope-remove", "Scope B", "server-b")
        );

        byte[] payload = codec.encode("realm", events, true,
                ClusterProvider.DCNotify.ALL_DCS, "node-1", "site-a");

        ValkeyClusterEventCodec.DecodedMessage decoded = codec.decode(payload);

        assertEquals(events.size(), decoded.events().size());
        assertIterableEquals(events, decoded.events());
    }

    @Test
    void shouldDefaultUnknownSiteFilterToAll() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos)) {
            writeString(out, "channel");
            out.writeBoolean(true);
            writeString(out, "node-1");
            out.writeBoolean(true);
            writeString(out, "site-a");
            out.writeByte(42);
            out.writeBoolean(false);
            out.writeInt(0);
        }

        ValkeyClusterEventCodec.DecodedMessage decoded = codec.decode(baos.toByteArray());
        assertTrue(decoded.shouldDeliver("node-2", "any"));
    }

    @Test
    void shouldRejectMissingEventKey() {
        assertThrows(IllegalStateException.class, () -> codec.decode(new byte[0]));
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static ClientRemovedEvent clientRemovedEvent(String id, String realmId, String clientId,
            Map<String, String> clientRoles) throws Exception {
        Constructor<ClientRemovedEvent> ctor = ClientRemovedEvent.class
                .getDeclaredConstructor(String.class, String.class, String.class, Map.class);
        ctor.setAccessible(true);
        return ctor.newInstance(id, realmId, clientId, new LinkedHashMap<>(clientRoles));
    }

    private static GroupMovedEvent groupMovedEvent(String id, String newParentId, String oldParentId, String realmId)
            throws Exception {
        Constructor<GroupMovedEvent> ctor = GroupMovedEvent.class
                .getDeclaredConstructor(String.class, String.class, String.class, String.class);
        ctor.setAccessible(true);
        return ctor.newInstance(id, newParentId, oldParentId, realmId);
    }

    private static UserFullInvalidationEvent userFullInvalidationEvent(String id, String username, String email,
            String realmId, boolean identityFederationEnabled, Map<String, String> federatedIdentities) throws Exception {
        Constructor<UserFullInvalidationEvent> ctor = UserFullInvalidationEvent.class.getDeclaredConstructor(String.class,
                String.class, String.class, String.class, boolean.class, Map.class);
        ctor.setAccessible(true);
        Map<String, String> identities = federatedIdentities == null ? null : new LinkedHashMap<>(federatedIdentities);
        return ctor.newInstance(id, username, email, realmId, identityFederationEnabled, identities);
    }

    private static UserFederationLinkRemovedEvent userFederationLinkRemovedEvent(String id, String realmId,
            String identityProviderId, String socialUserId) throws Exception {
        Constructor<UserFederationLinkRemovedEvent> ctor = UserFederationLinkRemovedEvent.class
                .getDeclaredConstructor(String.class, String.class, String.class, String.class);
        ctor.setAccessible(true);
        return ctor.newInstance(id, realmId, identityProviderId, socialUserId);
    }

    private UserStorageProviderModel sampleUserStorageProvider() {
        ComponentModel component = new ComponentModel();
        component.setId("provider-1");
        component.setName("ldap-provider");
        component.setProviderId("ldap");
        component.setProviderType(UserStorageProvider.class.getName());
        component.setConfig(new MultivaluedHashMap<>());
        component.getConfig().putSingle(UserStorageProviderModel.IMPORT_ENABLED, Boolean.TRUE.toString());
        component.getConfig().putSingle(UserStorageProviderModel.FULL_SYNC_PERIOD, "60");
        component.getConfig().putSingle(UserStorageProviderModel.CHANGED_SYNC_PERIOD, "120");

        return new UserStorageProviderModel(component);
    }
}
