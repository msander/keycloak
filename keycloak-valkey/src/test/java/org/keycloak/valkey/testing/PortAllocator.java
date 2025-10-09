package org.keycloak.valkey.testing;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * Utility to allocate an ephemeral TCP port.
 */
public final class PortAllocator {

    private PortAllocator() {
    }

    public static int findFreePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to find free TCP port", ex);
        }
    }
}
