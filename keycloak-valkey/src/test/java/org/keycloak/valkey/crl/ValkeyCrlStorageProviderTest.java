package org.keycloak.valkey.crl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.ByteArrayInputStream;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509CRL;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.keycloak.models.KeycloakSession;
import org.keycloak.valkey.ValkeyConnectionProvider;
import org.keycloak.valkey.connection.LettuceValkeyConnectionProviderFactory;
import org.keycloak.valkey.testing.EmbeddedValkeyServer;
import org.keycloak.valkey.testing.MapBackedConfigScope;
import org.mockito.Mockito;

class ValkeyCrlStorageProviderTest {

    private static final String SAMPLE_CRL_PEM = String.join("\n",
            "-----BEGIN X509 CRL-----",
            "MIIDHTCCAQUCAQEwDQYJKoZIhvcNAQELBQAwgYcxCzAJBgNVBAYTAlVTMQswCQYD",
            "VQQIDAJNQTEQMA4GA1UECgwHUmVkIEhhdDERMA8GA1UECwwIS2V5Y2xvYWsxITAf",
            "BgNVBAMMGEtleWNsb2FrIEludGVybWVkaWF0ZSBDQTEjMCEGCSqGSIb3DQEJARYU",
            "Y29udGFjdEBrZXljbG9hay5vcmcXDTI0MTIyMzA4MzkzNVoYDzIwNzQxMjExMDgz",
            "OTM1WjAVMBMCAhAJFw0xOTAzMTQxMTA5MzBaoDAwLjAfBgNVHSMEGDAWgBRHEnyJ",
            "C0dXGVQK9QMEzZ+GopZ2lDALBgNVHRQEBAICEAIwDQYJKoZIhvcNAQELBQADggIB",
            "AMT2ga8GxtQPRm1zteIB8/LMoJyqJ8llHBnnJvL7OK0hOYmCl1DizCV9+S02q53U",
            "v7gRarLBNkunNncOZ+662foODa6GoQvTDtQyXCNHmimtC3Gq4NaZ5GKVqBrfnZ0h",
            "KmmPS6E/WlWPoUT6gc7Tz1oG7EhJ2y6ywGadQ6V27hpBN8TEhvK7fg7/59n0Hjkh",
            "K0oDwdBqGHCo7z7bB6peF8p3xQOOiYgXeAU/BTVKBnMzj+QbkZgnjWpi9EXdCd/h",
            "mBmckHxC0r8M+HYdJzdBdEFXh+M2o99Q6mGqmLT8/2UtrMYsus57G+ShqlnwGC+T",
            "ZctSJZnGDmFuiAjfjZ7MX6dxIauDdTeMnk1yOPObPE3su/X6YIbZA8iPuJxqxN8c",
            "R9HQijHM1klI0u9FaEn1Wn30WWsAPjMq+wMMXv7PEr7tJOS/WFetC718v1bf+zVW",
            "I1m0b9EzsymIqbAy2uD42Xz5paRWVi4OAmE5ZtJnEmDV0gC0+aFCURND2kAqfgKN",
            "O3Vogb2i527Y8AJ4PkUki/3akkC7nwlYW36Pg7VYz+Xs39ZUz7bRL1Z5t0KzvXnf",
            "nGDwko198pieVBwBIgd0hubywsVT2frZ4mVMPLwHMgiLz6+MBg6z9w/y9ifptat+",
            "jCgV6bTUx1JWwOlRafLjE/txsEQPAuz2ePIjNVCpfr4d",
            "-----END X509 CRL-----");

    private static EmbeddedValkeyServer server;
    private static LettuceValkeyConnectionProviderFactory connectionFactory;

    private ValkeyConnectionProvider connectionProvider;
    private ValkeyCrlStorageProviderFactory storageFactory;
    private ValkeyCacheCrlProviderFactory cacheFactory;
    private KeycloakSession session;

    @BeforeAll
    static void startInfrastructure() {
        server = EmbeddedValkeyServer.builder()
                .startupTimeout(Duration.ofSeconds(5))
                .start();
        connectionFactory = new LettuceValkeyConnectionProviderFactory();
        Map<String, String> config = new HashMap<>();
        config.put("uri", "redis://" + server.getHost() + ':' + server.getPort());
        connectionFactory.init(MapBackedConfigScope.from(config));
    }

    @AfterAll
    static void shutdownInfrastructure() {
        if (connectionFactory != null) {
            connectionFactory.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @BeforeEach
    void setUp() {
        connectionProvider = connectionFactory.create(null);
        connectionProvider.getConnection().sync().flushall();
        session = Mockito.mock(KeycloakSession.class);
        Mockito.doReturn(connectionProvider).when(session).getProvider(ValkeyConnectionProvider.class);

        storageFactory = new ValkeyCrlStorageProviderFactory();
        Map<String, String> storageConfig = new HashMap<>();
        storageConfig.put("namespace", "test:crl");
        storageConfig.put("cacheTime", "60");
        storageConfig.put("minTimeBetweenRequests", "5");
        storageFactory.init(MapBackedConfigScope.from(storageConfig));

        cacheFactory = new ValkeyCacheCrlProviderFactory();
        cacheFactory.init(MapBackedConfigScope.from(storageConfig));
    }

    @AfterEach
    void tearDown() {
        if (connectionProvider != null) {
            connectionProvider.close();
        }
    }

    @Test
    void shouldCacheCrlEntries() throws Exception {
        ValkeyCrlStorageProvider provider = (ValkeyCrlStorageProvider) storageFactory.create(session);
        AtomicInteger loads = new AtomicInteger();
        Callable<X509CRL> loader = () -> {
            loads.incrementAndGet();
            return parseCrl(SAMPLE_CRL_PEM);
        };

        X509CRL first = provider.get("realm-crl", loader);
        assertNotNull(first);
        X509CRL second = provider.get("realm-crl", loader);
        assertNotNull(second);
        assertEquals(1, loads.get());
    }

    @Test
    void shouldRespectMinTimeBetweenRequests() throws Exception {
        ValkeyCrlStorageProvider provider = (ValkeyCrlStorageProvider) storageFactory.create(session);
        AtomicInteger loads = new AtomicInteger();
        Callable<X509CRL> loader = () -> {
            loads.incrementAndGet();
            return parseCrl(SAMPLE_CRL_PEM);
        };

        provider.get("realm-crl", loader);
        provider.refreshCache("realm-crl", loader);
        assertEquals(1, loads.get());
    }

    @Test
    void shouldExpireAfterCacheTime() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("namespace", "test:crl:expire");
        config.put("cacheTime", "1");
        config.put("minTimeBetweenRequests", "1");
        storageFactory.init(MapBackedConfigScope.from(config));

        ValkeyCrlStorageProvider provider = (ValkeyCrlStorageProvider) storageFactory.create(session);
        AtomicInteger loads = new AtomicInteger();
        Callable<X509CRL> loader = () -> {
            loads.incrementAndGet();
            return parseCrl(SAMPLE_CRL_PEM);
        };

        provider.get("expiring-crl", loader);
        Thread.sleep(1_200);
        provider.get("expiring-crl", loader);
        assertEquals(2, loads.get());
    }

    @Test
    void shouldClearCacheEntries() throws Exception {
        ValkeyCrlStorageProvider provider = (ValkeyCrlStorageProvider) storageFactory.create(session);
        ValkeyCacheCrlProvider cacheProvider = (ValkeyCacheCrlProvider) cacheFactory.create(session);
        AtomicInteger loads = new AtomicInteger();
        Callable<X509CRL> loader = () -> {
            loads.incrementAndGet();
            return parseCrl(SAMPLE_CRL_PEM);
        };

        provider.get("realm-crl", loader);
        cacheProvider.clearCache();
        provider.get("realm-crl", loader);
        assertEquals(2, loads.get());
    }

    private X509CRL parseCrl(String pem) throws GeneralSecurityException {
        CertificateFactory factory = CertificateFactory.getInstance("X.509");
        return (X509CRL) factory.generateCRL(new ByteArrayInputStream(pem.getBytes()));
    }
}
