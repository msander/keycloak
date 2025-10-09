package org.keycloak.valkey.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ValkeySpiMappingsTest {

    @Test
    void shouldExposeDescriptorsForEveryCache() {
        assertEquals(16, ValkeySpiMappings.descriptors().size());
        ValkeySpiMappings.descriptors().forEach(descriptor ->
                assertTrue(ValkeySpiMappings.byCacheName(descriptor.cacheName()).isPresent(),
                        () -> "Descriptor missing from lookup: " + descriptor.cacheName()));
    }

    @Test
    void descriptorsAreImmutable() {
        assertThrows(UnsupportedOperationException.class,
                () -> ValkeySpiMappings.descriptors().add(
                        new ValkeyCacheDescriptor("temp", "test", ValkeyCacheStructure.STRING,
                                "temp:{id}", ValkeyTtlPolicy.NONE, false, "temp")));
    }

    @Test
    void actionTokensHaveExpectedShape() {
        ValkeyCacheDescriptor descriptor = ValkeySpiMappings.byCacheName(ValkeySpiMappings.ACTION_TOKEN_CACHE)
                .orElseThrow();
        assertEquals(ValkeyCacheStructure.HASH, descriptor.structure());
        assertEquals(ValkeyTtlPolicy.ABSOLUTE_EXPIRATION, descriptor.ttlPolicy());
        assertTrue(descriptor.clustered());
        assertTrue(descriptor.notes().contains("sorted-set"));
    }

    @Test
    void workCacheUsesStreamAndClientManagedLifecycle() {
        ValkeyCacheDescriptor descriptor = ValkeySpiMappings.byCacheName(ValkeySpiMappings.WORK_CACHE)
                .orElseThrow();
        assertEquals(ValkeyCacheStructure.STREAM, descriptor.structure());
        assertEquals(ValkeyTtlPolicy.CLIENT_MANAGED, descriptor.ttlPolicy());
        assertTrue(descriptor.clustered());
    }

    @Test
    void lookupHandlesUnknownCaches() {
        assertTrue(ValkeySpiMappings.byCacheName("missing").isEmpty());
        assertTrue(ValkeySpiMappings.byCacheName(null).isEmpty());
    }
}
