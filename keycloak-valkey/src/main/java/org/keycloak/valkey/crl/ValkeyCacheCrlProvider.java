package org.keycloak.valkey.crl;

import java.util.List;
import java.util.Objects;

import org.keycloak.models.cache.CacheCrlProvider;
import org.keycloak.valkey.ValkeyConnectionProvider;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.sync.RedisCommands;

final class ValkeyCacheCrlProvider implements CacheCrlProvider {

    private final ValkeyConnectionProvider connectionProvider;
    private final ValkeyCrlStorageConfig config;

    ValkeyCacheCrlProvider(ValkeyConnectionProvider connectionProvider, ValkeyCrlStorageConfig config) {
        this.connectionProvider = Objects.requireNonNull(connectionProvider, "connectionProvider");
        this.config = Objects.requireNonNull(config, "config");
    }

    @Override
    public void clearCache() {
        RedisCommands<String, String> commands = connectionProvider.getConnection().sync();
        String pattern = config.namespace() + ":*";
        ScanArgs args = ScanArgs.Builder.matches(pattern).limit(200);
        ScanCursor cursor = ScanCursor.INITIAL;
        while (!cursor.isFinished()) {
            KeyScanCursor<String> scan = commands.scan(cursor, args);
            List<String> keys = scan.getKeys();
            if (!keys.isEmpty()) {
                commands.del(keys.toArray(new String[0]));
            }
            cursor = scan;
        }
    }

    @Override
    public void close() {
        // No-op
    }
}
