package org.keycloak.valkey.cluster;

import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.logging.Logger;
import org.keycloak.cluster.ClusterProvider;
import org.keycloak.models.KeycloakSession;

/**
 * Resolves the Valkey-backed {@link ClusterProvider}, falling back to the default provider configuration when available.
 */
public final class ValkeyClusterProviderResolver {

    private static final Logger logger = Logger.getLogger(ValkeyClusterProviderResolver.class);

    private ValkeyClusterProviderResolver() {
    }

    /**
     * Attempts to resolve the Valkey-backed {@link ClusterProvider}. The resolution order is:
     * <ol>
     * <li>The provider selected by the current Keycloak configuration.</li>
     * <li>The Valkey cluster provider identified by {@link ValkeyClusterProviderFactory#PROVIDER_ID}.</li>
     * </ol>
     *
     * @param session the active Keycloak session
     * @param clusterUnavailableLogged optional flag used to guard log emission when the provider cannot be resolved
     * @return the resolved cluster provider or {@code null} when none is available
     */
    public static ClusterProvider resolve(KeycloakSession session, AtomicBoolean clusterUnavailableLogged) {
        ClusterProvider cluster = session.getProvider(ClusterProvider.class);
        if (cluster == null) {
            cluster = session.getProvider(ClusterProvider.class, ValkeyClusterProviderFactory.PROVIDER_ID);
        }
        if (cluster == null) {
            if (clusterUnavailableLogged != null && clusterUnavailableLogged.compareAndSet(false, true)) {
                logger.warnf("Valkey cluster provider '%s' is unavailable; distributed notifications will be disabled.",
                        ValkeyClusterProviderFactory.PROVIDER_ID);
            }
            return null;
        }
        if (clusterUnavailableLogged != null) {
            clusterUnavailableLogged.set(false);
        }
        return cluster;
    }
}
