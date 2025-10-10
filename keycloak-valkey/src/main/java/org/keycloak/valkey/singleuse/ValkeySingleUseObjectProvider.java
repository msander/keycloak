package org.keycloak.valkey.singleuse;

import static org.keycloak.models.SingleUseObjectProvider.REVOKED_KEY;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.keycloak.models.KeycloakSession;
import org.keycloak.models.ModelException;
import org.keycloak.models.SingleUseObjectProvider;
import org.keycloak.models.session.RevokedTokenPersisterProvider;
import org.keycloak.valkey.ValkeyConnectionProvider;

import com.fasterxml.jackson.core.type.TypeReference;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.keycloak.util.JsonSerialization;

/**
 * {@link SingleUseObjectProvider} backed by Valkey using RESP commands with JSON payload encoding.
 */
final class ValkeySingleUseObjectProvider implements SingleUseObjectProvider {

    private static final TypeReference<Map<String, String>> MAP_TYPE = new TypeReference<>() {
    };
    private static final String ATOMIC_GETDEL_SCRIPT = "local value = redis.call('GET', KEYS[1]); "
            + "if value then redis.call('DEL', KEYS[1]); end; return value;";

    private final KeycloakSession session;
    private final RedisCommands<String, String> commands;
    private final boolean persistRevokedTokens;

    ValkeySingleUseObjectProvider(KeycloakSession session, ValkeyConnectionProvider connectionProvider,
            boolean persistRevokedTokens) {
        this.session = Objects.requireNonNull(session, "session");
        ValkeyConnectionProvider provider = Objects.requireNonNull(connectionProvider, "connectionProvider");
        StatefulRedisConnection<String, String> connection = provider.getConnection();
        this.commands = connection.sync();
        this.persistRevokedTokens = persistRevokedTokens;
    }

    @Override
    public void put(String key, long lifespanSeconds, Map<String, String> notes) {
        Map<String, String> payload = sanitize(notes);
        if (persistRevokedTokens && isRevokedKey(key) && !payload.isEmpty()) {
            throw new ModelException("Notes are not supported for revoked tokens");
        }
        setValue(key, payload, lifespanSeconds);
        if (persistRevokedTokens && isRevokedKey(key)) {
            revokeToken(key, lifespanSeconds);
        }
    }

    @Override
    public Map<String, String> get(String key) {
        if (persistRevokedTokens && isRevokedKey(key)) {
            throw new ModelException("Revoked tokens can't be retrieved");
        }
        return decode(commands.get(key));
    }

    @Override
    public Map<String, String> remove(String key) {
        if (persistRevokedTokens && isRevokedKey(key)) {
            throw new ModelException("Revoked tokens can't be removed");
        }
        return decode(getAndDelete(key));
    }

    @Override
    public boolean replace(String key, Map<String, String> notes) {
        if (persistRevokedTokens && isRevokedKey(key)) {
            throw new ModelException("Revoked tokens can't be replaced");
        }
        Map<String, String> payload = sanitize(notes);
        SetArgs args = new SetArgs().xx();
        long ttlMillis = commands.pttl(key);
        if (ttlMillis > 0) {
            args.px(ttlMillis);
        }
        String response = commands.set(key, encode(payload), args);
        return "OK".equals(response);
    }

    @Override
    public boolean putIfAbsent(String key, long lifespanInSeconds) {
        if (persistRevokedTokens && isRevokedKey(key)) {
            throw new ModelException("Revoked tokens can't be used in putIfAbsent");
        }
        long lifespan = requirePositiveLifespan(lifespanInSeconds);
        String response = commands.set(key, encode(Collections.emptyMap()), SetArgs.Builder.nx().ex(lifespan));
        return "OK".equals(response);
    }

    @Override
    public boolean contains(String key) {
        return commands.exists(key) > 0;
    }

    @Override
    public void close() {
        // Connection lifecycle is managed by ValkeyConnectionProvider.
    }

    private void setValue(String key, Map<String, String> payload, long lifespanSeconds) {
        long lifespan = requirePositiveLifespan(lifespanSeconds);
        String response = commands.set(key, encode(payload), SetArgs.Builder.ex(lifespan));
        if (!"OK".equals(response)) {
            throw new IllegalStateException("Failed to store single-use object in Valkey for key " + key);
        }
    }

    private void revokeToken(String key, long lifespanSeconds) {
        RevokedTokenPersisterProvider provider = session.getProvider(RevokedTokenPersisterProvider.class);
        if (provider == null) {
            throw new IllegalStateException("RevokedTokenPersisterProvider is required when persisting revoked tokens");
        }
        String tokenId = key.substring(0, key.length() - REVOKED_KEY.length());
        provider.revokeToken(tokenId, lifespanSeconds);
    }

    private static boolean isRevokedKey(String key) {
        return key != null && key.endsWith(REVOKED_KEY);
    }

    private static long requirePositiveLifespan(long lifespanSeconds) {
        if (lifespanSeconds <= 0) {
            throw new IllegalArgumentException("lifespanSeconds must be positive");
        }
        return lifespanSeconds;
    }

    private String getAndDelete(String key) {
        return commands.eval(ATOMIC_GETDEL_SCRIPT, ScriptOutputType.VALUE, new String[] {key});
    }

    private static Map<String, String> sanitize(Map<String, String> notes) {
        if (notes == null || notes.isEmpty()) {
            return Map.of();
        }
        Map<String, String> copy = new LinkedHashMap<>(notes);
        copy.values().removeIf(Objects::isNull);
        return copy.isEmpty() ? Map.of() : copy;
    }

    private static Map<String, String> decode(String payload) {
        if (payload == null) {
            return null;
        }
        if (payload.isEmpty()) {
            return Map.of();
        }
        try {
            Map<String, String> decoded = JsonSerialization.readValue(payload, MAP_TYPE);
            if (decoded == null || decoded.isEmpty()) {
                return Map.of();
            }
            return new LinkedHashMap<>(decoded);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to decode single-use object payload", ex);
        }
    }

    private static String encode(Map<String, String> notes) {
        try {
            return JsonSerialization.writeValueAsString(notes);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to encode single-use object payload", ex);
        }
    }
}
