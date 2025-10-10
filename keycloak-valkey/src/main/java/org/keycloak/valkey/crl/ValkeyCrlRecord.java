package org.keycloak.valkey.crl;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import org.keycloak.util.JsonSerialization;

final class ValkeyCrlRecord {

    private static final String FIELD_CRL = "crl";
    private static final String FIELD_LAST_REQUEST = "lastRequestTime";
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private final byte[] crlBytes;
    private final long lastRequestTime;

    private ValkeyCrlRecord(byte[] crlBytes, long lastRequestTime) {
        this.crlBytes = crlBytes;
        this.lastRequestTime = lastRequestTime;
    }

    static ValkeyCrlRecord create(byte[] crlBytes, long lastRequestTime) {
        return new ValkeyCrlRecord(crlBytes, lastRequestTime);
    }

    static ValkeyCrlRecord fromJson(String json) {
        if (json == null) {
            return null;
        }
        try {
            Map<String, Object> data = JsonSerialization.readValue(json, MAP_TYPE);
            Object payload = data.get(FIELD_CRL);
            if (!(payload instanceof String encoded)) {
                return null;
            }
            Object lastRequestValue = data.get(FIELD_LAST_REQUEST);
            long lastRequest = lastRequestValue != null ? Long.parseLong(lastRequestValue.toString()) : 0L;
            byte[] bytes = Base64.getDecoder().decode(encoded);
            return new ValkeyCrlRecord(bytes, lastRequest);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to decode Valkey CRL record", ex);
        }
    }

    String toJson() {
        Map<String, String> data = Map.of(
                FIELD_CRL, Base64.getEncoder().encodeToString(crlBytes),
                FIELD_LAST_REQUEST, Long.toString(lastRequestTime));
        try {
            return JsonSerialization.writeValueAsString(data);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to encode Valkey CRL record", ex);
        }
    }

    byte[] crlBytes() {
        return crlBytes;
    }

    long lastRequestTime() {
        return lastRequestTime;
    }
}
