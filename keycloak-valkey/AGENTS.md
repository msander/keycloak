# Agent Guidelines for `keycloak-valkey`

## Scope
These guidelines apply to every file inside the `keycloak-valkey` directory and its subdirectories.

## General Principles
- Treat this module as a self-contained Keycloak extension that must build as part of the main Maven reactor and produce deployable JAR artifacts.
- Maintain the planning state machine in `STATE.md`. Always update the plan, TODO list, and changelog whenever work is performed in this module.
- Prefer using Keycloak Service Provider Interfaces (SPIs) or public extension points; avoid modifying upstream Keycloak modules unless strictly necessary and explicitly approved.
- Structure code for maintainability, resilience, and performance. Favour clear separation between API contracts, implementation logic, and test utilities.

## Code Style
- Follow the existing Keycloak Java code conventions: use 4 spaces for indentation, no tabs.
- Keep classes in the package `org.keycloak.valkey` or subpackages.
- Do not introduce `try/catch` blocks solely to swallow exceptions; surface actionable errors with context.

## Testing
- Tests must run without Docker/Testcontainers. Use embedded or in-memory Valkey/Redis-compatible servers that can be started within the JVM.
- Provide utility helpers for spinning up the test Valkey instance to keep tests deterministic.

## Documentation
- Document architectural decisions and test strategies in `STATE.md` under dedicated sections.
- When adding public SPI implementations, include Javadoc summarising behaviour and configuration.

## Build
- Ensure the module declares dependencies so that upstream Keycloak artifacts it relies on are built first.
- Maven coordinates should use the `org.keycloak` groupId with an artifactId prefixed by `keycloak-`.
