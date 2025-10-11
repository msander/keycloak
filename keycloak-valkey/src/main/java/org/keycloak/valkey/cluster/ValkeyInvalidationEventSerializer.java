package org.keycloak.valkey.cluster;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.Set;

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
import org.keycloak.models.cache.infinispan.events.InvalidationEvent;
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
import org.keycloak.util.JsonSerialization;

/**
 * Serializer for Infinispan {@link InvalidationEvent} implementations. These events expose their payload
 * through package-private fields or constructors that are normally accessed via Protostream. The serializer
 * reflects over the event metadata so that Valkey-backed clusters can propagate cache invalidations without
 * depending on Protostream runtime support.
 */
public final class ValkeyInvalidationEventSerializer implements ValkeyClusterEventSerializer<InvalidationEvent> {

    private static final String TYPE_ID = "keycloak.valkey.cache.invalidation";

    private static final Constructor<ClientRemovedEvent> CLIENT_REMOVED_EVENT_CTOR =
            constructor(ClientRemovedEvent.class, String.class, String.class, String.class, Map.class);
    private static final Constructor<GroupMovedEvent> GROUP_MOVED_EVENT_CTOR =
            constructor(GroupMovedEvent.class, String.class, String.class, String.class, String.class);
    private static final Constructor<UserFullInvalidationEvent> USER_FULL_INVALIDATION_EVENT_CTOR =
            constructor(UserFullInvalidationEvent.class, String.class, String.class, String.class, String.class,
                    boolean.class, Map.class);
    private static final Constructor<UserFederationLinkRemovedEvent> USER_FEDERATION_LINK_REMOVED_EVENT_CTOR =
            constructor(UserFederationLinkRemovedEvent.class, String.class, String.class, String.class, String.class);

    private static final Map<Class<?>, EventAdapter<?>> ADAPTERS_BY_CLASS;
    private static final Map<String, EventAdapter<?>> ADAPTERS_BY_TYPE;

    static {
        Map<Class<?>, EventAdapter<?>> byClass = new LinkedHashMap<>();
        Map<String, EventAdapter<?>> byType = new LinkedHashMap<>();

        register(adapter("ROLE_ADDED", RoleAddedEvent.class,
                (event, attributes) -> {
                    attributes.put("containerId", readStringField(event, "containerId"));
                    attributes.put("roleName", readStringField(event, "roleName"));
                },
                attributes -> RoleAddedEvent.create(string(attributes, "id"),
                        string(attributes, "containerId"), string(attributes, "roleName"))),
                byClass, byType);

        register(adapter("ROLE_REMOVED", RoleRemovedEvent.class,
                (event, attributes) -> {
                    attributes.put("containerId", readStringField(event, "containerId"));
                    attributes.put("roleName", readStringField(event, "roleName"));
                },
                attributes -> RoleRemovedEvent.create(string(attributes, "id"),
                        string(attributes, "roleName"), string(attributes, "containerId"))),
                byClass, byType);

        register(adapter("ROLE_UPDATED", RoleUpdatedEvent.class,
                (event, attributes) -> {
                    attributes.put("containerId", readStringField(event, "containerId"));
                    attributes.put("roleName", readStringField(event, "roleName"));
                },
                attributes -> RoleUpdatedEvent.create(string(attributes, "id"),
                        string(attributes, "roleName"), string(attributes, "containerId"))),
                byClass, byType);

        register(adapter("CLIENT_ADDED", ClientAddedEvent.class,
                (event, attributes) -> attributes.put("realmId", readStringField(event, "realmId")),
                attributes -> ClientAddedEvent.create(string(attributes, "id"), string(attributes, "realmId"))),
                byClass, byType);

        register(adapter("CLIENT_UPDATED", ClientUpdatedEvent.class,
                (event, attributes) -> {
                    attributes.put("realmId", readStringField(event, "realmId"));
                    attributes.put("clientId", readStringField(event, "clientId"));
                },
                attributes -> ClientUpdatedEvent.create(string(attributes, "id"),
                        string(attributes, "clientId"), string(attributes, "realmId"))),
                byClass, byType);

        register(adapter("CLIENT_REMOVED", ClientRemovedEvent.class,
                (event, attributes) -> {
                    attributes.put("realmId", readStringField(event, "realmId"));
                    attributes.put("clientId", readStringField(event, "clientId"));
                    attributes.put("clientRoles", new LinkedHashMap<>(stringMap(event, "clientRoles")));
                },
                attributes -> newClientRemovedEvent(string(attributes, "id"), string(attributes, "realmId"),
                        string(attributes, "clientId"), stringMap(attributes, "clientRoles"))),
                byClass, byType);

        register(adapter("CLIENT_SCOPE_ADDED", ClientScopeAddedEvent.class,
                (event, attributes) -> attributes.put("realmId", readStringField(event, "realmId")),
                attributes -> ClientScopeAddedEvent.create(string(attributes, "id"), string(attributes, "realmId"))),
                byClass, byType);

        register(adapter("CLIENT_SCOPE_REMOVED", ClientScopeRemovedEvent.class,
                (event, attributes) -> attributes.put("realmId", readStringField(event, "realmId")),
                attributes -> ClientScopeRemovedEvent.create(string(attributes, "id"), string(attributes, "realmId"))),
                byClass, byType);

        register(adapter("AUTHZ_RESOURCE_SERVER_UPDATED", ResourceServerUpdatedEvent.class,
                (event, attributes) -> {
                },
                attributes -> ResourceServerUpdatedEvent.create(string(attributes, "id"))),
                byClass, byType);

        register(adapter("AUTHZ_RESOURCE_SERVER_REMOVED", ResourceServerRemovedEvent.class,
                (event, attributes) -> {
                },
                attributes -> ResourceServerRemovedEvent.create(string(attributes, "id"))),
                byClass, byType);

        register(adapter("AUTHZ_RESOURCE_UPDATED", ResourceUpdatedEvent.class,
                (event, attributes) -> {
                    attributes.put("name", readStringField(event, "name"));
                    attributes.put("owner", readStringField(event, "owner"));
                    attributes.put("serverId", readStringField(event, "serverId"));
                    attributes.put("type", readNullableStringField(event, "type"));
                    putSet(attributes, "uris", stringSetNullable(event, "uris"));
                    putSet(attributes, "scopes", stringSetNullable(event, "scopes"));
                },
                attributes -> ResourceUpdatedEvent.create(string(attributes, "id"), string(attributes, "name"),
                        nullableString(attributes, "type"), stringSetNullable(attributes, "uris"),
                        string(attributes, "owner"), stringSetNullable(attributes, "scopes"),
                        string(attributes, "serverId"))),
                byClass, byType);

        register(adapter("AUTHZ_RESOURCE_REMOVED", ResourceRemovedEvent.class,
                (event, attributes) -> {
                    attributes.put("name", readStringField(event, "name"));
                    attributes.put("owner", readStringField(event, "owner"));
                    attributes.put("serverId", readStringField(event, "serverId"));
                    attributes.put("type", readNullableStringField(event, "type"));
                    putSet(attributes, "uris", stringSetNullable(event, "uris"));
                    putSet(attributes, "scopes", stringSetNullable(event, "scopes"));
                },
                attributes -> ResourceRemovedEvent.create(string(attributes, "id"), string(attributes, "name"),
                        nullableString(attributes, "type"), stringSetNullable(attributes, "uris"),
                        string(attributes, "owner"), stringSetNullable(attributes, "scopes"),
                        string(attributes, "serverId"))),
                byClass, byType);

        register(adapter("AUTHZ_POLICY_UPDATED", PolicyUpdatedEvent.class,
                (event, attributes) -> {
                    attributes.put("name", readStringField(event, "name"));
                    attributes.put("serverId", readStringField(event, "serverId"));
                    putSet(attributes, "resources", stringSetNullable(event, "resources"));
                    putSet(attributes, "resourceTypes", stringSetNullable(event, "resourceTypes"));
                    putSet(attributes, "scopes", stringSetNullable(event, "scopes"));
                },
                attributes -> PolicyUpdatedEvent.create(string(attributes, "id"), string(attributes, "name"),
                        stringSetNullable(attributes, "resources"), stringSetNullable(attributes, "resourceTypes"),
                        stringSetNullable(attributes, "scopes"), string(attributes, "serverId"))),
                byClass, byType);

        register(adapter("AUTHZ_POLICY_REMOVED", PolicyRemovedEvent.class,
                (event, attributes) -> {
                    attributes.put("name", readStringField(event, "name"));
                    attributes.put("serverId", readStringField(event, "serverId"));
                    putSet(attributes, "resources", stringSetNullable(event, "resources"));
                    putSet(attributes, "resourceTypes", stringSetNullable(event, "resourceTypes"));
                    putSet(attributes, "scopes", stringSetNullable(event, "scopes"));
                },
                attributes -> PolicyRemovedEvent.create(string(attributes, "id"), string(attributes, "name"),
                        stringSetNullable(attributes, "resources"), stringSetNullable(attributes, "resourceTypes"),
                        stringSetNullable(attributes, "scopes"), string(attributes, "serverId"))),
                byClass, byType);

        register(adapter("AUTHZ_PERMISSION_TICKET_UPDATED", PermissionTicketUpdatedEvent.class,
                (event, attributes) -> {
                    attributes.put("owner", readStringField(event, "owner"));
                    attributes.put("resource", readStringField(event, "resource"));
                    attributes.put("scope", readStringField(event, "scope"));
                    attributes.put("serverId", readStringField(event, "serverId"));
                    attributes.put("requester", readNullableStringField(event, "requester"));
                    attributes.put("resourceName", readNullableStringField(event, "resourceName"));
                },
                attributes -> PermissionTicketUpdatedEvent.create(string(attributes, "id"),
                        string(attributes, "owner"), nullableString(attributes, "requester"),
                        string(attributes, "resource"), nullableString(attributes, "resourceName"),
                        string(attributes, "scope"), string(attributes, "serverId"))),
                byClass, byType);

        register(adapter("AUTHZ_PERMISSION_TICKET_REMOVED", PermissionTicketRemovedEvent.class,
                (event, attributes) -> {
                    attributes.put("owner", readStringField(event, "owner"));
                    attributes.put("resource", readStringField(event, "resource"));
                    attributes.put("scope", readStringField(event, "scope"));
                    attributes.put("serverId", readStringField(event, "serverId"));
                    attributes.put("requester", readNullableStringField(event, "requester"));
                    attributes.put("resourceName", readNullableStringField(event, "resourceName"));
                },
                attributes -> PermissionTicketRemovedEvent.create(string(attributes, "id"),
                        string(attributes, "owner"), nullableString(attributes, "requester"),
                        string(attributes, "resource"), nullableString(attributes, "resourceName"),
                        string(attributes, "scope"), string(attributes, "serverId"))),
                byClass, byType);

        register(adapter("AUTHZ_SCOPE_UPDATED", ScopeUpdatedEvent.class,
                (event, attributes) -> {
                    attributes.put("name", readStringField(event, "name"));
                    attributes.put("serverId", readStringField(event, "serverId"));
                },
                attributes -> ScopeUpdatedEvent.create(string(attributes, "id"), string(attributes, "name"),
                        string(attributes, "serverId"))),
                byClass, byType);

        register(adapter("AUTHZ_SCOPE_REMOVED", ScopeRemovedEvent.class,
                (event, attributes) -> {
                    attributes.put("name", readStringField(event, "name"));
                    attributes.put("serverId", readStringField(event, "serverId"));
                },
                attributes -> ScopeRemovedEvent.create(string(attributes, "id"), string(attributes, "name"),
                        string(attributes, "serverId"))),
                byClass, byType);

        register(adapter("REALM_UPDATED", RealmUpdatedEvent.class,
                (event, attributes) -> attributes.put("realmName", readStringField(event, "realmName")),
                attributes -> RealmUpdatedEvent.create(string(attributes, "id"), string(attributes, "realmName"))),
                byClass, byType);

        register(adapter("REALM_REMOVED", RealmRemovedEvent.class,
                (event, attributes) -> attributes.put("realmName", readStringField(event, "realmName")),
                attributes -> RealmRemovedEvent.create(string(attributes, "id"), string(attributes, "realmName"))),
                byClass, byType);

        register(adapter("GROUP_ADDED", GroupAddedEvent.class,
                (event, attributes) -> {
                    attributes.put("realmId", readStringField(event, "realmId"));
                    attributes.put("parentId", readNullableStringField(event, "parentId"));
                },
                attributes -> GroupAddedEvent.create(string(attributes, "id"),
                        nullableString(attributes, "parentId"), string(attributes, "realmId"))),
                byClass, byType);

        register(adapter("GROUP_REMOVED", GroupRemovedEvent.class,
                (event, attributes) -> {
                    attributes.put("realmId", readStringField(event, "realmId"));
                    attributes.put("parentId", readNullableStringField(event, "parentId"));
                },
                attributes -> new GroupRemovedEvent(string(attributes, "id"), string(attributes, "realmId"),
                        nullableString(attributes, "parentId"))),
                byClass, byType);

        register(adapter("GROUP_UPDATED", GroupUpdatedEvent.class,
                (event, attributes) -> {
                },
                attributes -> GroupUpdatedEvent.create(string(attributes, "id"))),
                byClass, byType);

        register(adapter("GROUP_MOVED", GroupMovedEvent.class,
                (event, attributes) -> {
                    attributes.put("newParentId", readNullableStringField(event, "newParentId"));
                    attributes.put("oldParentId", readNullableStringField(event, "oldParentId"));
                    attributes.put("realmId", readStringField(event, "realmId"));
                },
                attributes -> newGroupMovedEvent(string(attributes, "id"),
                        nullableString(attributes, "newParentId"), nullableString(attributes, "oldParentId"),
                        string(attributes, "realmId"))),
                byClass, byType);

        register(adapter("CACHE_KEY_INVALIDATED", CacheKeyInvalidatedEvent.class,
                (event, attributes) -> {
                },
                attributes -> new CacheKeyInvalidatedEvent(string(attributes, "id"))),
                byClass, byType);

        register(adapter("USER_UPDATED", UserUpdatedEvent.class,
                (event, attributes) -> {
                    attributes.put("username", readStringField(event, "username"));
                    attributes.put("email", readNullableStringField(event, "email"));
                    attributes.put("realmId", readStringField(event, "realmId"));
                },
                attributes -> UserUpdatedEvent.create(string(attributes, "id"), string(attributes, "username"),
                        nullableString(attributes, "email"), string(attributes, "realmId"))),
                byClass, byType);

        register(adapter("USER_FULL_INVALIDATION", UserFullInvalidationEvent.class,
                (event, attributes) -> {
                    attributes.put("username", readStringField(event, "username"));
                    attributes.put("email", readNullableStringField(event, "email"));
                    attributes.put("realmId", readStringField(event, "realmId"));
                    attributes.put("identityFederationEnabled", readBooleanField(event, "identityFederationEnabled"));
                    Map<String, String> identities = stringMapNullable(event, "federatedIdentities");
                    if (identities != null) {
                        attributes.put("federatedIdentities", new LinkedHashMap<>(identities));
                    }
                },
                attributes -> newUserFullInvalidationEvent(string(attributes, "id"),
                        string(attributes, "username"), nullableString(attributes, "email"),
                        string(attributes, "realmId"), bool(attributes, "identityFederationEnabled"),
                        stringMapNullable(attributes, "federatedIdentities"))),
                byClass, byType);

        register(adapter("USER_CONSENTS_UPDATED", UserConsentsUpdatedEvent.class,
                (event, attributes) -> {
                },
                attributes -> UserConsentsUpdatedEvent.create(string(attributes, "id"))),
                byClass, byType);

        register(adapter("USER_FEDERATION_LINK_UPDATED", UserFederationLinkUpdatedEvent.class,
                (event, attributes) -> {
                },
                attributes -> UserFederationLinkUpdatedEvent.create(string(attributes, "id"))),
                byClass, byType);

        register(adapter("USER_FEDERATION_LINK_REMOVED", UserFederationLinkRemovedEvent.class,
                (event, attributes) -> {
                    attributes.put("realmId", event.getRealmId());
                    attributes.put("identityProviderId", event.getIdentityProviderId());
                    attributes.put("socialUserId", event.getSocialUserId());
                },
                attributes -> newUserFederationLinkRemovedEvent(string(attributes, "id"),
                        string(attributes, "realmId"), nullableString(attributes, "identityProviderId"),
                        nullableString(attributes, "socialUserId"))),
                byClass, byType);

        register(adapter("USER_CACHE_REALM_INVALIDATION", UserCacheRealmInvalidationEvent.class,
                (event, attributes) -> {
                },
                attributes -> UserCacheRealmInvalidationEvent.create(string(attributes, "id"))),
                byClass, byType);

        ADAPTERS_BY_CLASS = Map.copyOf(byClass);
        ADAPTERS_BY_TYPE = Map.copyOf(byType);
    }

    @Override
    public String getTypeId() {
        return TYPE_ID;
    }

    @Override
    public Class<InvalidationEvent> getEventType() {
        return InvalidationEvent.class;
    }

    @Override
    public byte[] serialize(InvalidationEvent event) throws IOException {
        EventAdapter<InvalidationEvent> adapter = findAdapter(event.getClass());
        Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put("id", event.getId());
        adapter.writer.accept(event, attributes);
        Payload payload = new Payload(adapter.type, attributes);
        return JsonSerialization.writeValueAsBytes(payload);
    }

    @Override
    public InvalidationEvent deserialize(byte[] payload) throws IOException {
        Payload data = JsonSerialization.readValue(payload, Payload.class);
        if (data.type == null) {
            throw new IllegalStateException("Missing invalidation event type in payload");
        }
        EventAdapter<?> adapter = ADAPTERS_BY_TYPE.get(data.type);
        if (adapter == null) {
            throw new IllegalStateException("Unknown invalidation event type " + data.type);
        }
        Map<String, Object> attributes = data.attributes != null ? new LinkedHashMap<>(data.attributes)
                : new LinkedHashMap<>();
        if (!attributes.containsKey("id")) {
            throw new IllegalStateException("Invalidation event payload missing id field for type " + data.type);
        }
        @SuppressWarnings("unchecked")
        EventAdapter<InvalidationEvent> typed = (EventAdapter<InvalidationEvent>) adapter;
        return typed.reader.apply(attributes);
    }

    @SuppressWarnings("unchecked")
    private static EventAdapter<InvalidationEvent> findAdapter(Class<?> eventClass) {
        Class<?> current = eventClass;
        while (current != null) {
            EventAdapter<?> adapter = ADAPTERS_BY_CLASS.get(current);
            if (adapter != null) {
                return (EventAdapter<InvalidationEvent>) adapter;
            }
            current = current.getSuperclass();
        }
        throw new IllegalStateException("No invalidation event adapter registered for class " + eventClass.getName());
    }

    private static <E extends InvalidationEvent> EventAdapter<E> adapter(String type, Class<E> eventClass,
            BiConsumer<E, Map<String, Object>> writer, Function<Map<String, Object>, E> reader) {
        return new EventAdapter<>(type, eventClass, writer, reader);
    }

    private static <E extends InvalidationEvent> void register(EventAdapter<E> adapter,
            Map<Class<?>, EventAdapter<?>> byClass, Map<String, EventAdapter<?>> byType) {
        EventAdapter<?> existing = byClass.putIfAbsent(adapter.eventClass, adapter);
        if (existing != null) {
            throw new IllegalStateException("Duplicate invalidation event adapter for class "
                    + adapter.eventClass.getName());
        }
        EventAdapter<?> conflict = byType.putIfAbsent(adapter.type, adapter);
        if (conflict != null) {
            throw new IllegalStateException("Duplicate invalidation event adapter type " + adapter.type);
        }
    }

    private static String string(Map<String, Object> attributes, String key) {
        Object value = attributes.get(key);
        if (value instanceof String string) {
            return string;
        }
        throw new IllegalStateException("Expected string attribute '" + key + "' but found " + value);
    }

    private static String nullableString(Map<String, Object> attributes, String key) {
        Object value = attributes.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof String string) {
            return string;
        }
        throw new IllegalStateException("Expected string attribute '" + key + "' but found " + value);
    }

    private static Set<String> stringSetNullable(Map<String, Object> attributes, String key) {
        Object value = attributes.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof Collection<?> collection) {
            return copyToStringSet(collection, key);
        }
        throw new IllegalStateException("Expected collection attribute '" + key + "' but found " + value);
    }

    private static boolean bool(Map<String, Object> attributes, String key) {
        Object value = attributes.get(key);
        if (value instanceof Boolean bool) {
            return bool;
        }
        throw new IllegalStateException("Expected boolean attribute '" + key + "' but found " + value);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> stringMap(Map<String, Object> attributes, String key) {
        Object value = attributes.get(key);
        if (value instanceof Map<?, ?> map) {
            Map<String, String> result = new LinkedHashMap<>();
            map.forEach((k, v) -> {
                if (!(k instanceof String) || !(v instanceof String)) {
                    throw new IllegalStateException("Expected string map for attribute '" + key + "'");
                }
                result.put((String) k, (String) v);
            });
            return result;
        }
        throw new IllegalStateException("Expected map attribute '" + key + "' but found " + value);
    }

    private static Map<String, String> stringMapNullable(Map<String, Object> attributes, String key) {
        Object value = attributes.get(key);
        if (value == null) {
            return null;
        }
        return stringMap(attributes, key);
    }

    private static void putSet(Map<String, Object> attributes, String key, Set<String> values) {
        if (values != null) {
            attributes.put(key, new ArrayList<>(values));
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> stringMap(Object event, String fieldName) {
        Map<String, String> map = (Map<String, String>) readField(event, fieldName);
        if (map == null) {
            throw new IllegalStateException("Field '" + fieldName + "' returned null on " + event.getClass());
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> stringMapNullable(Object event, String fieldName) {
        Object value = readField(event, fieldName);
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, String> result = new LinkedHashMap<>();
            map.forEach((k, v) -> {
                if (!(k instanceof String) || !(v instanceof String)) {
                    throw new IllegalStateException("Expected string map for field '" + fieldName + "'");
                }
                result.put((String) k, (String) v);
            });
            return result;
        }
        throw new IllegalStateException("Field '" + fieldName + "' not a map on " + value);
    }

    private static Set<String> stringSetNullable(Object event, String fieldName) {
        Object value = readField(event, fieldName);
        if (value == null) {
            return null;
        }
        if (value instanceof Set<?> set) {
            return copyToStringSet(set, fieldName);
        }
        throw new IllegalStateException("Field '" + fieldName + "' not a set on " + event.getClass());
    }

    private static Set<String> copyToStringSet(Collection<?> source, String fieldOrKey) {
        Set<String> result = new LinkedHashSet<>();
        for (Object element : source) {
            if (!(element instanceof String)) {
                throw new IllegalStateException("Expected string elements for '" + fieldOrKey + "'");
            }
            result.add((String) element);
        }
        return result;
    }

    private static String readStringField(Object event, String fieldName) {
        Object value = readField(event, fieldName);
        if (value instanceof String string) {
            return string;
        }
        throw new IllegalStateException("Field '" + fieldName + "' not a string on " + event.getClass());
    }

    private static String readNullableStringField(Object event, String fieldName) {
        Object value = readField(event, fieldName);
        if (value == null) {
            return null;
        }
        if (value instanceof String string) {
            return string;
        }
        throw new IllegalStateException("Field '" + fieldName + "' not a string on " + event.getClass());
    }

    private static boolean readBooleanField(Object event, String fieldName) {
        Object value = readField(event, fieldName);
        if (value instanceof Boolean bool) {
            return bool;
        }
        throw new IllegalStateException("Field '" + fieldName + "' not a boolean on " + event.getClass());
    }

    private static Object readField(Object target, String fieldName) {
        try {
            return resolveField(target.getClass(), fieldName).get(target);
        } catch (IllegalAccessException ex) {
            throw new IllegalStateException("Unable to access field '" + fieldName + "' on " + target.getClass(), ex);
        }
    }

    private static Field resolveField(Class<?> type, String fieldName) {
        String key = type.getName() + "#" + fieldName;
        return FIELD_CACHE.computeIfAbsent(key, ignored -> locateField(type, fieldName));
    }

    private static Field locateField(Class<?> type, String fieldName) {
        Class<?> current = type;
        while (current != null) {
            try {
                Field field = current.getDeclaredField(fieldName);
                field.setAccessible(true);
                return field;
            } catch (NoSuchFieldException ignored) {
                current = current.getSuperclass();
            }
        }
        throw new IllegalStateException("Field '" + fieldName + "' not found on " + type.getName());
    }

    private static <T> Constructor<T> constructor(Class<T> type, Class<?>... parameterTypes) {
        try {
            Constructor<T> constructor = type.getDeclaredConstructor(parameterTypes);
            constructor.setAccessible(true);
            return constructor;
        } catch (NoSuchMethodException ex) {
            throw new IllegalStateException("Unable to resolve constructor for " + type.getName(), ex);
        }
    }

    private static ClientRemovedEvent newClientRemovedEvent(String id, String realmId, String clientId,
            Map<String, String> clientRoles) {
        return invokeConstructor(CLIENT_REMOVED_EVENT_CTOR, id, realmId, clientId, clientRoles);
    }

    private static GroupMovedEvent newGroupMovedEvent(String id, String newParentId, String oldParentId, String realmId) {
        return invokeConstructor(GROUP_MOVED_EVENT_CTOR, id, newParentId, oldParentId, realmId);
    }

    private static UserFullInvalidationEvent newUserFullInvalidationEvent(String id, String username, String email,
            String realmId, boolean identityFederationEnabled, Map<String, String> federatedIdentities) {
        return invokeConstructor(USER_FULL_INVALIDATION_EVENT_CTOR, id, username, email, realmId,
                identityFederationEnabled, federatedIdentities);
    }

    private static UserFederationLinkRemovedEvent newUserFederationLinkRemovedEvent(String id, String realmId,
            String identityProviderId, String socialUserId) {
        return invokeConstructor(USER_FEDERATION_LINK_REMOVED_EVENT_CTOR, id, realmId, identityProviderId,
                socialUserId);
    }

    private static <T> T invokeConstructor(Constructor<T> constructor, Object... args) {
        try {
            return constructor.newInstance(args);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException ex) {
            throw new IllegalStateException("Unable to construct " + constructor.getDeclaringClass().getName(), ex);
        }
    }

    private static final Map<String, Field> FIELD_CACHE = new ConcurrentHashMap<>();

    private static final class Payload {
        public String type;
        public Map<String, Object> attributes;

        Payload() {
        }

        Payload(String type, Map<String, Object> attributes) {
            this.type = Objects.requireNonNull(type);
            this.attributes = Objects.requireNonNull(attributes);
        }
    }

    private static final class EventAdapter<E extends InvalidationEvent> {
        private final String type;
        private final Class<E> eventClass;
        private final BiConsumer<E, Map<String, Object>> writer;
        private final Function<Map<String, Object>, E> reader;

        EventAdapter(String type, Class<E> eventClass, BiConsumer<E, Map<String, Object>> writer,
                Function<Map<String, Object>, E> reader) {
            this.type = type;
            this.eventClass = eventClass;
            this.writer = writer;
            this.reader = reader;
        }
    }
}
