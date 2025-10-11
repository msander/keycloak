package org.keycloak.valkey.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.keycloak.common.util.MultivaluedHashMap;
import org.keycloak.component.ComponentModel;
import org.keycloak.storage.UserStorageProviderModel;
import org.keycloak.storage.managers.UserStorageSyncManager.UserStorageProviderClusterEvent;
import org.keycloak.util.JsonSerialization;

/**
 * Serializer for {@link UserStorageProviderClusterEvent} instances to propagate user storage sync updates
 * across the cluster without relying on Protostream.
 */
public final class UserStorageProviderClusterEventSerializer
        implements ValkeyClusterEventSerializer<UserStorageProviderClusterEvent> {

    private static final String TYPE_ID = "keycloak.valkey.user-storage-provider";

    @Override
    public String getTypeId() {
        return TYPE_ID;
    }

    @Override
    public Class<UserStorageProviderClusterEvent> getEventType() {
        return UserStorageProviderClusterEvent.class;
    }

    @Override
    public byte[] serialize(UserStorageProviderClusterEvent event) throws IOException {
        Payload payload = new Payload(event.isRemoved(), event.getRealmId(), ProviderPayload.fromModel(event.getStorageProvider()));
        return JsonSerialization.writeValueAsBytes(payload);
    }

    @Override
    public UserStorageProviderClusterEvent deserialize(byte[] payload) throws IOException {
        Payload data = JsonSerialization.readValue(payload, Payload.class);
        return UserStorageProviderClusterEvent.createEvent(data.removed, data.realmId, data.storageProvider.toModel());
    }

    private static final class Payload {
        public boolean removed;
        public String realmId;
        public ProviderPayload storageProvider;

        Payload() {
        }

        Payload(boolean removed, String realmId, ProviderPayload storageProvider) {
            this.removed = removed;
            this.realmId = realmId;
            this.storageProvider = storageProvider;
        }
    }

    private static final class ProviderPayload {
        public String id;
        public String name;
        public String providerId;
        public String providerType;
        public String parentId;
        public String subType;
        public Map<String, List<String>> config;

        ProviderPayload() {
        }

        static ProviderPayload fromModel(UserStorageProviderModel model) {
            ProviderPayload payload = new ProviderPayload();
            payload.id = model.getId();
            payload.name = model.getName();
            payload.providerId = model.getProviderId();
            payload.providerType = model.getProviderType();
            payload.parentId = model.getParentId();
            payload.subType = model.getSubType();
            payload.config = new LinkedHashMap<>();
            model.getConfig().forEach((key, values) -> {
                if (values == null || values.isEmpty()) {
                    payload.config.put(key, List.of());
                } else {
                    payload.config.put(key, new ArrayList<>(values));
                }
            });
            return payload;
        }

        UserStorageProviderModel toModel() {
            ComponentModel component = new ComponentModel();
            component.setId(id);
            component.setName(name);
            component.setProviderId(providerId);
            component.setProviderType(providerType);
            component.setParentId(parentId);
            component.setSubType(subType);
            MultivaluedHashMap<String, String> configMap = new MultivaluedHashMap<>();
            if (config != null) {
                config.forEach((key, values) -> {
                    List<String> copy = values == null ? new ArrayList<>() : new ArrayList<>(values);
                    configMap.put(key, copy);
                });
            }
            component.setConfig(configMap);
            return new UserStorageProviderModel(component);
        }
    }
}
