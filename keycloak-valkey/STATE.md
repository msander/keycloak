# Keycloak Valkey Extension State

## Vision & Scope
Replace the default Infinispan-based clustering layers in Keycloak with a Redis/Valkey/KeyDB backed solution that can be distributed as drop-in extension JARs. The solution must use existing Service Provider Interfaces (SPIs) wherever possible so that no upstream Keycloak sources need to be patched.

## Current Status
- Repository scaffolding created under `keycloak-valkey/` with project guidelines (`AGENTS.md`).
- Maven module participates in the main reactor and now ships a functional Lettuce-backed `ValkeyConnectionProviderFactory` registered through the Keycloak SPI loader.
 - Embedded Valkey test harness ensures integration tests can run without Docker/Testcontainers; initial connection-provider tests exercise string and binary command paths.
 - Connection factory now reports sanitised configuration details and live health diagnostics via `ServerInfoAwareProviderFactory`.
- Cluster provider delivers distributed lock semantics backed by Valkey with pub/sub propagation for cross-node listener notifications, including multi-site filtering and resilient message decoding while remaining agnostic of Infinispan-specific event classes.
- DB lock provider integrates the global database lock SPI with Valkey, supporting forced unlock semantics and configurable retry/lease controls.

## Architectural Assumptions
1. All clustering/storage touch points (distributed caches, action tokens, work cache, authorization, user sessions, offline sessions, login failures, clients, authentication sessions) can be replaced via Keycloak's `MapStorageProvider`, `HotRodConnectionProvider`, or dedicated SPI alternatives.
2. Redis-compatible backends (Valkey, Redis OSS, KeyDB) will be accessed via the RESP protocol using a Java client such as Lettuce for async/reactive capabilities and cluster support.
3. No Testcontainers/Docker availability; integration tests must rely on an embedded or in-process Redis-compatible server.
4. Extension artifacts must be publishable as JARs that can be copied to `providers/` in a Keycloak distribution.

## Implementation Plan
1. **Module & Build Setup**
   - Add `keycloak-valkey` as Maven module under the repository root to ensure it participates in the main reactor.
   - Inside module, structure subpackages for `spi`, `provider`, `session`, `config`, and `test-support`.
   - Declare dependencies on necessary Keycloak SPIs (`server-spi`, `server-spi-private`, `model`, `services`) with `provided` scope where appropriate; include Lettuce (or Valkey-native client) as runtime dependency.
   - Configure the module to produce a shaded provider JAR bundling the Valkey client while keeping Keycloak dependencies as provided.

2. **SPI Strategy & Provider Design**
   - Harden the new `ValkeyConnectionProviderFactory` with operational metrics and configuration validation feedback.
   - Implement a custom `MapStorageProviderFactory` and `MapStorageProvider` that back Keycloak map storages with Redis data structures, leveraging existing map storage abstractions to avoid modifying built-in components.
   - Supply `LockProviderFactory` and `ClusterProviderFactory` implementations to replace Infinispan for distributed locks and cluster node discovery, reusing Keycloak SPI contracts.
   - Provide caches for action tokens and short-lived data via the `ShortLivedTaskExecutor` SPI or existing caches bridging through `MapKeycloakTransaction`.
   - Add configuration properties (namespace prefix, connection URI, SSL, authentication, topology options) exposed through `META-INF/services` and `module.properties`.

3. **Session & Cache Mapping**
   - Define serialization strategy using JSON/Protostream equivalents stored in Redis hashes, with TTL handling for ephemeral entries.
   - Implement consistent key scheme with domain-specific prefixes (`user-session:realm:sessionId` etc.) and encode value payloads using Keycloak's existing serialization utilities when available.
   - Handle cross-data center replication by supporting Redis Cluster or Valkey multi-master setups; include configuration toggles for enabling read replicas.

4. **Startup Lifecycle Integration**
   - Register providers via `META-INF/services/org.keycloak.provider.ProviderFactory` and `org.keycloak.Config.Scope` metadata.
   - On startup, initialize connection pools (Lettuce `StatefulConnection`), run health checks, and optionally perform schema bootstrap (creating Lua scripts for atomic ops).
   - Ensure graceful shutdown flushes connection pools and unlocks distributed locks.

5. **Resilience & Observability**
   - Integrate circuit breaking and retry semantics using Lettuce `RetryPolicy`; expose metrics via Keycloak's Micrometer integration (if available) or custom SPI events.
   - Implement reconnect/backoff handling and validation for misconfiguration.
   - Provide admin events or log warnings when backend latency or error thresholds exceeded.

6. **Testing Strategy**
   - Unit tests covering serialization, key composition, and SPI wiring using mocks.
   - Integration tests leveraging the embedded Valkey server harness introduced in this iteration (no Docker/Testcontainers).
   - Provide fallback stub for environments lacking native binaries by implementing a minimal RESP server tailored for tests (future TODO if embedded binary insufficient).

7. **Documentation & Operations**
   - Supply usage guide (README) describing installation, configuration properties, recommended deployment topologies, and migration steps from Infinispan.
   - Document limitations and known compatibility considerations (e.g., eviction policies, maximum TTL support).
   - Provide migration utilities or scripts to export/import sessions from Infinispan caches where feasible.

## TODO Backlog
- [x] Add `keycloak-valkey` Maven module with initial `pom.xml` and placeholder source set to ensure compilation.
- [x] Define dependency management for Lettuce/Valkey client and embedded test server in the module POM.
- [ ] Draft high-level component diagram illustrating provider replacements.
- [x] Prototype embedded Redis server bootstrapping utility for tests (no Docker/Testcontainers).
- [ ] Flesh out SPI mapping table (which Keycloak caches map to which Redis structures) within this document.
- [ ] Implement actual provider classes following the plan (future work), focusing next on map storage and cache replacements.
- [x] Provide Valkey-backed DB lock provider with forced unlock support and configurable lease/retry settings.
- [x] Extend connection subsystem with operational health reporting hooks and publish readiness diagnostics.
- [x] Provide Valkey-backed cluster provider with distributed locking and local listener dispatch.
- [x] Implement cross-node cluster notifications using Valkey pub/sub to replace the current local-only dispatch.
- [x] Expose configuration options for multi-site routing (local/remote DC) and document prerequisites for site naming.
- [ ] Document cluster pub/sub configuration semantics and operational recommendations for site-aware deployments.
- [ ] Extract reusable protostream schemas for cluster events into a neutral module so Infinispan is no longer required on the classpath.
- [ ] Derive reusable metrics facade (Micrometer integration) for downstream providers.
- [ ] Evaluate adaptive lock lease tuning and observability for the DB lock provider (latency metrics, failure alarms).

## Testing Notes
- Goal is to run `mvn -pl keycloak-valkey test` without external services. Embedded server must start on random free port and clean up after tests.
- Evaluate deterministic seed data and concurrency scenarios to ensure session consistency during failover.

## Change Log
- **v0.7.0-dblock**: Added a Valkey-backed global DB lock provider with configurable timeouts, startup forced-unlock support, and comprehensive embedded Valkey tests.
- **v0.6.1-decouple-infinispan**: Removed the direct build dependency on `keycloak-model-infinispan`, introduced test-local protostream schema/fixtures, and documented the path toward neutral cluster-event serialization.
- **v0.6.0-cluster-pubsub-filters**: Hardened cluster pub/sub with Protostream codec abstraction, site-aware delivery filters, and expanded integration/unit tests for cross-DC behaviour.
- **v0.5.0-cluster-pubsub**: Added Valkey pub/sub propagation for cluster events with protostream-based serialization, singleton provider lifecycle, and cross-factory test coverage.
- **v0.4.0-cluster-locks**: Introduced the Valkey-backed cluster provider with Redis-based locks, asynchronous coordination helpers, and expanded test coverage.
- **v0.3.0-operational**: Added configuration validation, health probing with operational metadata exposure, and accompanying unit/integration tests.
- **v0.2.0-connections**: Introduced Lettuce-backed `ValkeyConnectionProvider` SPI implementation, registered factory/service descriptors, and verified connectivity using the embedded Valkey harness.
- **v0.1.1-skeleton**: Added Maven module to parent build, declared dependencies (Lettuce client, embedded Valkey test server), and verified successful packaging.
- **v0.1.0-plan**: Initial planning document, guidelines, and module scaffolding TODOs.
