package org.keycloak.valkey.testing;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import redis.embedded.RedisServer;
import redis.embedded.core.RedisServerBuilder;

/**
 * Starts an embedded Valkey/Redis compatible server for integration tests without relying on Docker/Testcontainers.
 */
public final class EmbeddedValkeyServer implements AutoCloseable {

    private static final Duration DEFAULT_STARTUP_TIMEOUT = Duration.ofSeconds(10);

    private final RedisServer redisServer;
    private final String host;
    private final int port;
    private final Duration startupTimeout;

    private EmbeddedValkeyServer(RedisServer redisServer, String host, int port, Duration startupTimeout) {
        this.redisServer = redisServer;
        this.host = host;
        this.port = port;
        this.startupTimeout = startupTimeout;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static EmbeddedValkeyServer startWithDefaults() {
        return builder().start();
    }

    public int getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }

    @Override
    public void close() {
        if (redisServer != null && redisServer.isActive()) {
            try {
                redisServer.stop();
            } catch (IOException ex) {
                throw new IllegalStateException("Failed to stop embedded Valkey", ex);
            }
        }
    }

    private void awaitReady() {
        long deadline = System.nanoTime() + startupTimeout.toNanos();
        while (System.nanoTime() < deadline) {
            try (Socket socket = new Socket()) {
                SocketAddress address = new InetSocketAddress(host, port);
                int connectTimeout = (int) Math.min(TimeUnit.SECONDS.toMillis(1), startupTimeout.toMillis());
                socket.connect(address, Math.max(connectTimeout, 200));
                return;
            } catch (IOException ignored) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting for embedded Valkey to start", ie);
                }
            }
        }
        throw new IllegalStateException("Embedded Valkey server did not start within " + startupTimeout);
    }

    public static final class Builder {
        private String bindAddress = "127.0.0.1";
        private int port = 0;
        private Duration startupTimeout = DEFAULT_STARTUP_TIMEOUT;
        private boolean disablePersistence = true;

        public Builder bindAddress(String bindAddress) {
            this.bindAddress = Objects.requireNonNull(bindAddress, "bindAddress");
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder startupTimeout(Duration timeout) {
            this.startupTimeout = Objects.requireNonNull(timeout, "timeout");
            return this;
        }

        public Builder disablePersistence(boolean disablePersistence) {
            this.disablePersistence = disablePersistence;
            return this;
        }

        public EmbeddedValkeyServer start() {
            int actualPort = port == 0 ? PortAllocator.findFreePort() : port;
            RedisServerBuilder builder = RedisServer.newRedisServer()
                    .bind(bindAddress)
                    .port(actualPort)
                    .setting("daemonize no");
            if (disablePersistence) {
                builder.setting("save \"\"");
                builder.setting("appendonly no");
            }
            try {
                RedisServer server = builder.build();
                EmbeddedValkeyServer wrapper = new EmbeddedValkeyServer(server, bindAddress, actualPort, startupTimeout);
                server.start();
                wrapper.awaitReady();
                return wrapper;
            } catch (IOException ex) {
                throw new IllegalStateException("Failed to start embedded Valkey", ex);
            }
        }
    }
}
