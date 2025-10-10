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
- Datastore provider factory now wraps the default store managers to prefer Valkey-backed providers and validates prerequisite Valkey infrastructure.
- Single-use object provider backed by Valkey stores distributed action tokens with atomic removal semantics and optional revoked-token persistence.
- User login failure provider persists brute-force counters in Valkey hashes with monotonic updates and TTL enforcement aligned with realm policies.
- Authentication session provider stores root sessions and per-tab authentication state in Valkey hashes with optimistic updates and TTL derived from realm lifespans.
- User session provider persists online and offline user sessions together with client sessions in Valkey hashes, enforcing realm lifespans/idle timeouts via TTL and optimistic transactions.
- User session persister stores durable online and offline sessions plus client session metadata in Valkey with query indexes for counts, pagination, and expiry management.
- Public key cache now uses a Valkey-aware storage provider with cluster invalidation events, local cache clearing hooks, and Valkey-native event serializers that avoid Protostream dependencies.
- Certificate revocation list cache and storage providers back CRL resolution with Valkey, exposing namespace configuration and respecting TTL/min-refresh policies.
- Workflow state provider stores scheduled workflow steps in Valkey hashes with sorted-set indexes to support due-step scans and indexed lookups, backed by embedded Valkey tests.

## Architectural Assumptions
1. All clustering/storage touch points (distributed caches, action tokens, work cache, authorization, user sessions, offline sessions, login failures, clients, authentication sessions) will be integrated through dedicated SPI implementations (e.g., `UserSessionProvider`, `AuthenticationSessionProvider`, `UserLoginFailureProvider`) and existing extension hooks without relying on the deprecated Map Storage architecture.
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
   - Deliver dedicated provider factories for each Keycloak SPI we are replacing (e.g., `UserSessionProviderFactory`, `AuthenticationSessionProviderFactory`, `UserLoginFailureProviderFactory`, `SingleUseObjectProviderFactory`) so that Valkey-backed implementations plug in directly without the Map Storage adapter layer.
   - Supply `LockProviderFactory` and `ClusterProviderFactory` implementations to replace Infinispan for distributed locks and cluster node discovery, reusing Keycloak SPI contracts.
   - Provide caches for action tokens and short-lived data via the `ShortLivedTaskExecutor` SPI or existing caches bridging through transactional hooks exposed by the native SPI contracts.
   - Add configuration properties (namespace prefix, connection URI, SSL, authentication, topology options) exposed through `META-INF/services` and `module.properties`.

3. **Session & Cache Mapping**
   - Target the distributed caches (authentication sessions, user sessions, action tokens, login failures, work cache) as they directly impact cluster consistency; local caches remain handled by the embedded Keycloak process and stay with their default providers.
   - Define serialization strategy using JSON or Valkey-native serializer SPIs stored in Redis hashes, with TTL handling for ephemeral entries.
   - Implement consistent key scheme with domain-specific prefixes (`user-session:realm:sessionId` etc.) and encode value payloads using Keycloak's existing serialization utilities when available.
   - Handle cross-data center replication by supporting Redis Cluster or Valkey multi-master setups; include configuration toggles for enabling read replicas.

### SPI Mapping Catalogue

| Keycloak cache / SPI | Valkey structure | Key pattern | TTL policy | Cluster scope | Notes |
| --- | --- | --- | --- | --- | --- |
| `realms` / RealmCacheProvider | Hash | `realm:{realmId}` | None (revision invalidated) | Local | Realm metadata stored as structured documents and invalidated via the paired revision cache. |
| `realmRevisions` / RealmCacheProvider | String | `realm-rev:{realmId}` | None | Local | Tracks the monotonic revision for realm cache entries so remote nodes can evict on change. |
| `users` / UserCacheProvider | Hash | `user:{realmId}:{userId}` | None (revision invalidated) | Local | Mirrors cached user metadata; revisions drive invalidation and eliminate global TTL churn. |
| `userRevisions` / UserCacheProvider | String | `user-rev:{realmId}:{userId}` | None | Local | Revision counter supporting cross-node invalidation of user cache entries. |
| `authorization` / AuthorizationProvider | Hash | `authz:{realmId}:{resourceId}` | None (revision invalidated) | Local | Authorization objects cached per resource/policy and coordinated with revision entries. |
| `authorizationRevisions` / AuthorizationProvider | String | `authz-rev:{realmId}:{resourceId}` | None | Local | Revision counter for authorization cache entries. |
| `keys` / PublicKeyStorageProvider | Hash | `keys:{realmId}:{keyId}` | Absolute expiry (max idle) | Local | Public keys and cert chains cached with their configured lifespan for predictable rotation. |
| `crl` / TruststoreProvider | Hash | `crl:{realmId}:{kid}` | Absolute expiry (max idle) | Local | Certificate revocation lists cached with deterministic expiry per truststore policy. |
| `sessions` / UserSessionProvider | Hash | `user-session:{realmId}:{sessionId}` | Absolute expiry (session lifespan) | Clustered | Online user sessions replicated across the cluster with TTL aligned to session expiration. |
| `clientSessions` / UserSessionProvider | Hash | `client-session:{realmId}:{sessionId}` | Absolute expiry (session lifespan) | Clustered | Client session records tied to user sessions and expiring in lock-step. |
| `offlineSessions` / UserSessionProvider | Hash | `offline-user-session:{realmId}:{sessionId}` | Absolute expiry (offline lifespan) | Clustered | Offline user sessions leveraging TTL derived from offline session policies. |
| `offlineClientSessions` / UserSessionProvider | Hash | `offline-client-session:{realmId}:{sessionId}` | Absolute expiry (offline lifespan) | Clustered | Offline client sessions using the same TTL and eviction semantics as their user counterparts. |
| `loginFailures` / UserLoginFailureProvider | Hash | `login-failure:{realmId}:{userId}` | Absolute expiry (per realm policy) | Clustered | Failed login counters expire according to the configured wait interval for lockout recovery. |
| `authenticationSessions` / AuthenticationSessionProvider | Hash | `auth-session:{realmId}:{rootSessionId}` | Absolute expiry (auth session lifespan) | Clustered | Root authentication sessions expire according to authentication lifespan settings. |
| `actionTokens` / SingleUseObjectProvider | Hash + Sorted set index | `action-token:{tokenId}` | Absolute expiry (token lifespan) | Clustered | Single-use tokens stored as hash payloads with a sorted-set index to support efficient sweeps. |
| `work` / ClusterProvider (WorkCache) | Stream | `work:{realmId}` | Client managed | Clustered | Cluster task queue implemented via Redis Streams with consumer groups for node coordination. |
| `workflowState` / WorkflowStateProvider | Hash + Sorted set indexes | `workflow-state:{realmId}:{executionId}` | Client managed | Clustered | Scheduled workflow steps persisted as hashes with workflow/resource/step indexes for due-step scans. |

> **Note:** Local cache replacements (realm, user, authorization metadata) are intentionally out of scope for this extension because they are embedded within each Keycloak node. The focus is ensuring cluster-visible data uses Valkey-backed providers with strong consistency semantics.

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
- [x] Flesh out SPI mapping table (which Keycloak caches map to which Redis structures) within this document.
- [x] Implement user session provider backed by Valkey, covering online/offline sessions and attached client sessions with optimistic persistence and TTL alignment.
- [ ] Implement remaining clustered session providers (work cache, user/client session cross-DC import, etc.) building on the user session infrastructure.
- [x] Provide Valkey-backed DB lock provider with forced unlock support and configurable lease/retry settings.
- [x] Extend connection subsystem with operational health reporting hooks and publish readiness diagnostics.
- [x] Provide Valkey-backed cluster provider with distributed locking and local listener dispatch.
- [x] Implement cross-node cluster notifications using Valkey pub/sub to replace the current local-only dispatch.
- [x] Expose configuration options for multi-site routing (local/remote DC) and document prerequisites for site naming.
- [ ] Document cluster pub/sub configuration semantics and operational recommendations for site-aware deployments.
- [x] Replace protostream-based cluster event encoding with a Valkey-native serializer SPI to eliminate Infinispan dependencies.
- [x] Provide Valkey-backed workflow state provider with sorted-set indexes for scheduling and deterministic tests.
- [ ] Derive reusable metrics facade (Micrometer integration) for downstream providers.
- [ ] Evaluate adaptive lock lease tuning and observability for the DB lock provider (latency metrics, failure alarms).

## Testing Notes
- Goal is to run `mvn -pl keycloak-valkey test` without external services. Embedded server must start on random free port and clean up after tests.
- Evaluate deterministic seed data and concurrency scenarios to ensure session consistency during failover.

## Change Log
- **v0.8.10-crl-storage**: Introduced Valkey-backed CRL storage and cache providers with Valkey persistence, TTL enforcement, and embedded tests.
- **v0.8.9-workflow-state**: Added a Valkey workflow state provider that maintains sorted-set indexes for due step scheduling with comprehensive embedded Valkey tests.
- **v0.8.8-cluster-serialization**: Replaced protostream codecs with a Valkey-native cluster event serializer SPI, removed the Infinispan dependency, and wired dedicated serializers for public key invalidation and cache clearing events.
- **v0.8.7-public-keys**: Added Valkey-backed public key storage and cache providers with protostream-encoded cluster invalidations, local cache clearing support, and concurrency-focused unit tests.
- **v0.8.6-session-persister**: Introduced a Valkey user session persister with indexed storage for session/client lookups, expiration handling, and embedded tests covering counts, offline retrieval, and cleanup.
- **v0.8.5-user-sessions**: Added a Valkey-backed user session provider with online/offline session storage, client session attachments, TTL-aware optimistic updates, and embedded lifecycle tests.
- **v0.8.4-auth-sessions**: Added a Valkey-backed authentication session provider with optimistic Valkey persistence, configurable session limits, and embedded tests covering auth note propagation and root session lifecycle.
- **v0.8.3-plan-refresh**: Reworked the architectural plan to replace Map Storage dependencies with native SPI provider implementations for Valkey-backed services.
- **v0.8.2-login-failures**: Added a Valkey-backed user login failure provider with atomic counter updates, configurable namespaces, and embedded Valkey tests validating TTL and clearing semantics.
- **v0.8.1-single-use**: Added a Valkey-backed single-use object provider with JSON payload encoding, scripted atomic removal, and revoked-token preload support to keep distributed action tokens in Valkey.
- **v0.8.0-datastore**: Introduced a Valkey datastore provider factory that enforces Valkey connection/cluster prerequisites and prefers Valkey-backed SPI implementations, covered by targeted unit tests.
- **v0.7.1-spi-mapping**: Documented the SPI-to-Valkey data structure catalogue and codified descriptors covering all clustered and local caches.
- **v0.7.0-dblock**: Added a Valkey-backed global DB lock provider with configurable timeouts, startup forced-unlock support, and comprehensive embedded Valkey tests.
- **v0.6.1-decouple-infinispan**: Removed the direct build dependency on `keycloak-model-infinispan`, introduced test-local protostream schema/fixtures, and documented the path toward neutral cluster-event serialization.
- **v0.6.0-cluster-pubsub-filters**: Hardened cluster pub/sub with Protostream codec abstraction, site-aware delivery filters, and expanded integration/unit tests for cross-DC behaviour.
- **v0.5.0-cluster-pubsub**: Added Valkey pub/sub propagation for cluster events with protostream-based serialization, singleton provider lifecycle, and cross-factory test coverage.
- **v0.4.0-cluster-locks**: Introduced the Valkey-backed cluster provider with Redis-based locks, asynchronous coordination helpers, and expanded test coverage.
- **v0.3.0-operational**: Added configuration validation, health probing with operational metadata exposure, and accompanying unit/integration tests.
- **v0.2.0-connections**: Introduced Lettuce-backed `ValkeyConnectionProvider` SPI implementation, registered factory/service descriptors, and verified connectivity using the embedded Valkey harness.
- **v0.1.1-skeleton**: Added Maven module to parent build, declared dependencies (Lettuce client, embedded Valkey test server), and verified successful packaging.
- **v0.1.0-plan**: Initial planning document, guidelines, and module scaffolding TODOs.
