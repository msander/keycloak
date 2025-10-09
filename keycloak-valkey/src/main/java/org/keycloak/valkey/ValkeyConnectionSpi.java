package org.keycloak.valkey;

import org.keycloak.provider.Spi;

/**
 * SPI descriptor for Valkey connection providers.
 */
public class ValkeyConnectionSpi implements Spi {

    public static final String SPI_NAME = "connectionsValkey";

    @Override
    public boolean isInternal() {
        return true;
    }

    @Override
    public String getName() {
        return SPI_NAME;
    }

    @Override
    public Class<ValkeyConnectionProvider> getProviderClass() {
        return ValkeyConnectionProvider.class;
    }

    @Override
    public Class<ValkeyConnectionProviderFactory> getProviderFactoryClass() {
        return ValkeyConnectionProviderFactory.class;
    }
}
