package org.keycloak.valkey;

import org.keycloak.provider.ProviderFactory;

/**
 * Base contract for factories that create {@link ValkeyConnectionProvider} instances.
 */
public interface ValkeyConnectionProviderFactory extends ProviderFactory<ValkeyConnectionProvider> {
}
