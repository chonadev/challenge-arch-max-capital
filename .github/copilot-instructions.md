# Copilot instructions for challenge-arch-max-capital

## Project overview

This repo is a Spring Boot / Kafka / PostgreSQL service that ingests ExecutionReports (`er.raw`), applies them to order state, and emits settlement events via an outbox pattern. The design source of truth is `SPEC.md`; `DECISIONS.md` captures the key trade-offs and rationale.

The core flow is:
- `ExecutionReportListener` consumes `er.raw` and validates the message.
- `OrderProcessingService` applies each ER inside one database transaction.
- Postgres stores both order state and the execution ledger, with idempotency enforced at the database layer.
- When an order reaches `FILLED`, `OutboxPoller` publishes settlement events to the `settlement` topic.
- Several integration tests exercise the production-critical invariants using real Postgres + Kafka containers.

## Build, test, and validation commands

### Run the app stack

```bash
docker compose up -d --build
```

This starts Postgres, Kafka, and two instances of the service. The API is served on `http://localhost:8081` and `http://localhost:8082`.

### Run the full test suite

```bash
mvn test
```

This is the main validation command. It uses Testcontainers with real Postgres and Kafka containers, so Docker must be running.

### Run a single test class

```bash
mvn -Dtest=OutboxClaimTest test
```

### Run a single test method

```bash
mvn -Dtest=OutboxClaimTest#claimReleasesForStaleLockedRow test
```

### Reset local DB state after a migration/schema change

```bash
docker compose down -v
docker compose up -d --build
```

There is no separate lint target in this repository; `mvn test` is the expected verification gate for code changes.

## High-level architecture

### Message ingestion and validation

- `src/main/java/com/maxcapital/orderprocessing/kafka/ExecutionReportListener.java` is the Kafka consumer for `er.raw`.
- It validates mandatory ER fields and uses Spring's retryable topic configuration for transient failures.
- `PermanentProcessingException` and `InvalidStateTransitionException` are treated as permanent and are sent directly to `er.raw.dlq` without extra retry loops.

### Order processing transaction

- `OrderProcessingService` is the main domain logic entry point.
- It applies each ER in one transaction:
  1. lock the order row with `SELECT ... FOR UPDATE`
  2. insert into the execution ledger with idempotent DB semantics (`ON CONFLICT DO NOTHING`)
  3. validate and update the order state from the persisted state
  4. emit a settlement event to the outbox only when the new status is `FILLED`

This is the critical place to look when fixing bugs around duplicate ERs, invalid transitions, or interleaved order processing.

### Database model and schema

- `src/main/resources/db/migration/V1__init_schema.sql` defines the relational schema including `orders`, `execution_ledger`, and `outbox`.
- JPA entities live under `src/main/java/com/maxcapital/orderprocessing/model` and are backed by PostgreSQL tables.
- The `Order` entity tracks status, leaves, accumulative nominal, and execution counts; the logic is intentionally incremental rather than overriding the previous state.

### Outbox / settlement publication

- `OutboxPoller` publishes events from the `outbox` table to Kafka.
- This avoids the dual-write problem between DB state and broker publication.
- The `locked_at` claim mechanism is used to prevent two service instances from publishing the same pending settlement row concurrently, with stale claims recovered after a timeout.
- `OutboxRepositoryService` is the helper for claim/release logic; do not treat `SKIP LOCKED` alone as sufficient for multi-instance safety.

### Kafka partitioning and concurrency model

- Kafka partitions by `numericOrderId` so all ERs for the same order stay in the same partition, preserving per-order sequence.
- The app is designed to scale horizontally across two service instances while preserving the required intra-order ordering guarantees.
- The app does not attempt to guarantee exactly-once delivery across Postgres + Kafka; it instead uses idempotent DB writes and outbox retries to make the flow safe and recoverable.

## Key conventions in this codebase

- `numericOrderId` is the logical order key and also the Kafka message key.
- The deduplication key for the execution ledger is `(secondaryTradeId, operationNumber)`, not `fixId`.
- The first ER for a brand-new order must be `NEW`; anything else is treated as a permanent processing error.
- A status transition is validated against the currently persisted status before it is applied; terminal states (`FILLED`, `CANCELLED`) reject further ERs.
- `CANCELLED` orders do not emit settlement events; only `FILLED` triggers an outbox record.
- Tests are not mock-heavy. They intentionally hit Postgres and Kafka through Testcontainers to validate the behavior that depends on real DB semantics (locks, UNIQUE constraints, `ON CONFLICT`, and outbox claims).
- The source of truth for behavior and guarantees is `SPEC.md`; when a change affects business invariants, check the spec before changing logic or tests.

## Working patterns to preserve

- Prefer changing the domain logic in `OrderProcessingService` and `Order` rather than patching around the symptom in controller or listener code.
- Keep event-processing semantics atomic: if a change touches state and outbox publication, it must preserve the transactional ordering and recovery assumptions described above.
- When you add or modify tests, match the existing integration style in `src/test/java/com/maxcapital/orderprocessing` and validate the real invariant being protected.

## Useful reference files

- `README.md` — service startup and scenario guidance
- `SPEC.md` — business guarantees and acceptance criteria
- `DECISIONS.md` — design rationale for Kafka, Postgres, idempotency, and outbox handling
- `src/main/resources/application.yml` — runtime config for DB and Kafka
- `src/main/java/com/maxcapital/orderprocessing/service/OrderProcessingService.java` — transaction handler for ERs
- `src/main/java/com/maxcapital/orderprocessing/service/OutboxPoller.java` — settlement publication loop
- `src/test/java/com/maxcapital/orderprocessing/*` — integration tests covering the critical invariants
