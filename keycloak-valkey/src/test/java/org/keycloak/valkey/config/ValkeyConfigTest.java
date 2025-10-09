package org.keycloak.valkey.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;

import org.junit.jupiter.api.Test;

class ValkeyConfigTest {

    @Test
    void shouldRejectBlankUri() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new ValkeyConfig("  ", null, Duration.ofSeconds(1), Duration.ofSeconds(1),
                        Duration.ofMillis(10), Duration.ofMillis(10), false, false, true, 1));
        assertEquals("Valkey URI must not be blank", ex.getMessage());
    }

    @Test
    void shouldRejectNonPositiveTimeouts() {
        assertThrows(IllegalArgumentException.class,
                () -> new ValkeyConfig("redis://localhost", null, Duration.ZERO, Duration.ofSeconds(1),
                        Duration.ofMillis(10), Duration.ofMillis(10), false, false, true, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new ValkeyConfig("redis://localhost", null, Duration.ofSeconds(1), Duration.ZERO,
                        Duration.ofMillis(10), Duration.ofMillis(10), false, false, true, 1));
    }

    @Test
    void shouldRejectNegativeQueueSize() {
        assertThrows(IllegalArgumentException.class,
                () -> new ValkeyConfig("redis://localhost", null, Duration.ofSeconds(1), Duration.ofSeconds(1),
                        Duration.ofMillis(10), Duration.ofMillis(10), false, false, true, 0));
    }

    @Test
    void shouldMaskUriCredentials() {
        ValkeyConfig config = new ValkeyConfig("redis://user:secret@localhost:6379", "client", Duration.ofSeconds(1),
                Duration.ofSeconds(1), Duration.ofMillis(10), Duration.ofMillis(10), false, false, true, 32);
        assertEquals("redis://***@localhost:6379", config.getSanitizedUri());
    }
}
