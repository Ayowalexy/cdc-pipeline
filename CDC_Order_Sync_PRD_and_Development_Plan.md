# PRD & Development Plan: CDC Order Synchronization

**Role:** Software & AI/ML Engineering (DSD) --- Backend / Distributed
Systems Engineer\
**Stack:** NestJS, TypeScript, PostgreSQL, CDC, Elasticsearch, Docker
Compose, Kibana, pgAdmin\
**Purpose:** Implementation-ready specification for a coding LLM to
build the local starter project, reproducible defect, correction, tests,
and deliverables.

## 1. Executive summary

Build a small order-management system where PostgreSQL is the source of
truth and Elasticsearch is an asynchronously maintained search
projection. Database changes flow through a genuine CDC pipeline into a
NestJS consumer, which updates Elasticsearch.

The exercise centers on one deliberately seeded defect: under a
deterministic event sequence, an older order update can overwrite a
newer Elasticsearch document. The engineer must reproduce the mismatch,
inspect PostgreSQL through pgAdmin and Elasticsearch through Kibana/Dev
Tools, trace the event path, implement a targeted fix, and validate
normal updates, rapid updates, duplicate delivery, consumer
restart/replay, out-of-order events, and deletion.

The project must run locally through Docker Compose, use synthetic data
only, and include a reliable before-fix reproduction. Avoid random
timing-dependent defects.

## 2. Goals, non-goals, and success criteria

### Goals

1.  Provide a realistic but compact PostgreSQL-to-Elasticsearch CDC
    architecture.
2.  Run required infrastructure locally through Docker Compose.
3.  Seed one deterministic stale-document defect in the
    consumer/indexing path.
4.  Make source and indexed state easy to inspect.
5.  Implement atomic version-aware protection against stale writes.
6.  Validate all six required scenarios with automated and manual
    evidence.
7.  Produce `test_matrix.xlsx`, evidence files, and
    `incident_report.md`.

### Non-goals

-   A complete commerce platform, authentication, payments, checkout, or
    customer UI.
-   Cloud deployment, Kubernetes, production-scale benchmarking, or a
    general CDC framework.
-   Claiming exactly-once delivery across independent systems.
-   Adding unrelated bugs or large amounts of application functionality.

### Definition of done

-   Docker Compose starts the documented services.
-   Normal order changes reach Elasticsearch.
-   The pre-fix defect reproduces deterministically.
-   Root cause is supported by logs, event metadata, and code.
-   The fix prevents stale events from regressing indexed state.
-   Duplicate delivery and replay are safe for tested scenarios.
-   Deletion is reflected and stale updates cannot resurrect a deleted
    order within the defined retention model.
-   Automated tests pass and all deliverables accurately reflect
    observed results.

## 3. Architecture and technology requirements

### Preferred topology

**NestJS Order Service → PostgreSQL → Debezium PostgreSQL Connector →
Kafka → NestJS CDC Consumer → Elasticsearch**

Kibana is used to inspect Elasticsearch. pgAdmin is used to inspect
PostgreSQL. Docker Compose orchestrates local infrastructure.

Include only supporting Kafka services required by the selected Kafka
distribution. Pin compatible image versions. Configure PostgreSQL
logical replication if using Debezium.

**Architecture integrity rule:** inspect the actual repository first. If
it uses another CDC mechanism, document that honestly. Do not claim
Debezium/Kafka is present unless it is actually implemented.
Connector-specific event parsing must be isolated behind an adapter.

### Suggested order model

-   `id`: stable UUID or synthetic order ID.
-   `status`: `PENDING`, `CONFIRMED`, `PROCESSING`, `COMPLETED`,
    `CANCELLED`.
-   `totalAmount`: synthetic numeric amount; no payment integration.
-   `currency`: optional ISO code.
-   `version`: monotonically increasing integer per order.
-   `createdAt`, `updatedAt`: timestamps.

Increment `version` atomically for every state-changing update. Do not
rely solely on wall-clock timestamps for event ordering.

### Minimal API

-   `POST /orders` --- create synthetic order.
-   `GET /orders/:id` --- retrieve source record.
-   `PATCH /orders/:id/status` --- update status and increment version.
-   `DELETE /orders/:id` --- delete order and emit the appropriate CDC
    delete/tombstone event.
-   `GET /health` --- health check.

### Normalized event contract

Normalize connector-specific payloads into an internal event,
conceptually:

``` ts
type OrderChangeEvent = {
  eventId: string;
  aggregateType: 'order';
  aggregateId: string;
  operation: 'CREATE' | 'UPDATE' | 'DELETE';
  version: number;
  occurredAt: string;
  sourcePosition?: {
    transactionId?: string;
    lsn?: string;
    partition?: number;
    offset?: string;
  };
  after: OrderSnapshot | null;
};
```

The exact Debezium envelope varies by connector version. Build and test
an adapter for the pinned configuration. Log event ID, order ID,
operation, version, source position where available, and outcome. Never
log secrets.

### Elasticsearch projection

Create an `orders` index with explicit mappings: - `order_id`:
keyword. - `status`: keyword. - `version`: numeric. - `created_at`,
`updated_at`: date. - Other fields only when useful.

Use the PostgreSQL order ID as the deterministic Elasticsearch document
ID. Include status and version in each indexed document.

Useful Dev Tools query:

``` http
GET orders/_doc/ORD-1001
```

Also document a search query and a query that verifies deleted records
are excluded.

## 4. Seeded defect and deterministic reproduction

### Defect statement

The initial consumer uses unsafe indexing behavior that allows an older
event to overwrite a newer Elasticsearch document.

Example sequence: 1. Version 2: `CONFIRMED`. 2. Version 3: `PROCESSING`.
3. Version 4: `COMPLETED`. 4. A controlled delivery/replay applies
version 2 after version 4. 5. Before the fix, Elasticsearch ends at a
stale status while PostgreSQL remains at version 4 / `COMPLETED`.

Implement one primary defect only. A suitable seed is an unconditional
index/upsert operation with no atomic version guard. Use a controlled
event dispatcher or fixture to force the older event to arrive late. Do
not depend on random sleeps or a timing-sensitive race.

### Reproduction requirements

Provide `scripts/reproduce-stale-update.ts` or equivalent. It must: -
Reset/create a known synthetic order. - Execute a documented sequence. -
Wait for processing through bounded polling or explicit synchronization,
not arbitrary sleeps. - Print PostgreSQL and Elasticsearch final
states. - Fail clearly before the fix when states differ. - Pass after
the fix. - Be safe to run repeatedly.

The README must document the exact command and expected before/after
behavior.

## 5. Fix requirements

### Atomic version-aware indexing

For each order: - Higher version may replace lower version. - Equal
version is a duplicate/no-op, unless an explicitly safe equivalent is
defined. - Lower version must never overwrite higher version. - The
compare-and-write must be atomic.

Preferred approaches: 1. Elasticsearch external versioning using the
order version and suitable strict version semantics; or 2. An
Elasticsearch scripted update that atomically compares incoming and
stored versions.

Choose one based on the existing architecture and explain the decision.
**Do not implement a read-then-write check in application code**,
because it introduces its own concurrency race.

### Duplicate delivery

-   Reprocessing the same event must not regress the document.
-   Log duplicate/stale outcomes distinctly.
-   Do not introduce non-idempotent external side effects.

### Offset/checkpoint handling

Acknowledge/commit an event only after its processing outcome is safely
established according to the selected consumer library. Document: -
Commit timing. - Retry behavior. - Handling of Elasticsearch version
conflicts. - Transient versus permanent errors. - What happens after
process failure.

Do not claim exactly-once processing. Describe actual delivery semantics
and the idempotency strategy.

### Deletion and stale resurrection

Physical deletion removes the document's version state. A later stale
update could recreate it. Select and document a defensible strategy. For
this exercise, a versioned tombstone document is acceptable: - Store
`is_deleted: true` and the latest version. - Ensure application search
queries filter deleted documents. - Reject older updates against the
tombstone. - Explain that tombstone cleanup needs a retention policy and
must wait until the replay horizon has passed.

Test deletion followed by delivery of an older update.

### Observability

Use structured logs containing: - `eventId`, `orderId`, `operation`. -
`incomingVersion`, `indexedVersion` where known. - Outcome: `applied`,
`ignored_stale`, `duplicate`, `deleted`, `retryable_error`, or
`permanent_error`. - Correlation ID if available.

## 6. Non-functional requirements

-   PostgreSQL remains authoritative; Elasticsearch is an eventually
    consistent projection.
-   The system converges after successful processing.
-   Older events cannot regress an order's indexed state.
-   Health checks and startup dependencies are configured where
    practical.
-   `.env.example` documents local configuration; no real credentials.
-   Setup is repeatable from a clean checkout.
-   Keep connector parsing separate from projection logic.
-   Avoid unnecessary abstractions and unrelated refactoring.
-   Make failure and retry behavior observable.

## 7. Test strategy and matrix

Required manual and automated scenarios:

  --------------------------------------------------------------------------
  ID                Scenario          Procedure         Expected result
  ----------------- ----------------- ----------------- --------------------
  TC-01             Normal update     Update an order   Elasticsearch
                                      once              converges to new
                                                        status/version

  TC-02             Rapid updates     Apply several     Final indexed
                                      status changes    version/status
                                      quickly           matches PostgreSQL

  TC-03             Duplicate event   Deliver the same  No regression;
                                      event more than   duplicate safely
                                      once              ignored/idempotent

  TC-04             Consumer          Stop with event   Correct
                    restart/replay    pending or use    offset/replay
                                      controlled        behavior and final
                                      replay, then      state
                                      restart           

  TC-05             Out-of-order      Apply higher      Lower version cannot
                    event             version, then     overwrite higher
                                      lower version     version

  TC-06             Deletion          Delete order,     Order remains
                                      then deliver      deleted/not
                                      stale update      searchable
  --------------------------------------------------------------------------

For each case record: - ID, scenario, preconditions, exact
actions/commands. - Expected and actual PostgreSQL state. - Expected and
actual Elasticsearch state. - Pass/fail, evidence filename(s), notes.

Automated coverage should include: - Event normalization unit tests. -
Version decision tests if logic is separated. - Elasticsearch
integration tests for atomic version handling. - Regression test for the
seeded defect. - Duplicate, out-of-order, deletion/tombstone, and stale
resurrection tests. - Consumer recovery/replay test that accurately
models the selected broker's offset behavior.

Use real Elasticsearch integration tests where the task claims to
validate Elasticsearch behavior. Reset test data between cases. Use
bounded polling for eventual consistency. Do not treat normal indexing
delay as a defect.

## 8. Manual recording workflow

The screen recording should show this sequence: 1. Introduce the system
and explain the architecture. 2. Show repository and Docker Compose
configuration. 3. Start containers and verify health. 4. Inspect a
synthetic order in pgAdmin. 5. Inspect the matching Elasticsearch
document in Kibana/Dev Tools. 6. Run the deterministic reproduction
before the fix. 7. Show PostgreSQL/Elasticsearch mismatch and relevant
logs. 8. Inspect event metadata and trace the consumer path. 9. Explain
the root cause before changing code. 10. Implement the targeted fix and
explain why it is safe. 11. Run the same reproduction again. 12. Execute
all six regression scenarios. 13. Inspect final source and indexed
states. 14. Update the test matrix and capture evidence. 15. Complete
the incident report. 16. Summarize results and remaining limitations.

Narrate engineering reasoning, not every mouse click. Do not claim a
replay occurred unless the offset/event behavior proves it.

## 9. Deliverables

### Code

-   Updated consumer/indexing logic.
-   Minimal supporting adapter/types/configuration.
-   Automated regression tests.
-   No unrelated refactoring.

### `test_matrix.xlsx`

Sheet 1 --- Test Results: - Test ID, scenario, preconditions, steps. -
Expected result, actual result. - PostgreSQL result, Elasticsearch
result. - Pass/fail, evidence references, notes.

Sheet 2 --- Evidence Index: - Evidence ID, test ID, filename,
description. - Before/after, run identifier or timestamp.

Do not invent results or evidence references.

### `incident_report.md`

Required sections: 1. Executive summary. 2. Architecture/data flow. 3.
Observed symptom. 4. Reproduction steps. 5. Root cause with code/event
evidence. 6. Fix and design rationale. 7. Validation summary and matrix
reference. 8. Evidence filenames. 9. Remaining risks/limitations. 10.
Conclusion and deployment recommendation.

Clearly distinguish tested from untested behavior. Do not claim
production readiness based only on local tests.

### Evidence directory

``` text
deliverables/
  before/
    before_postgres.png
    before_elasticsearch.png
    before_consumer_logs.txt
  after/
    after_postgres.png
    after_elasticsearch.png
    after_consumer_logs.txt
  test_matrix.xlsx
  incident_report.md
```

Use actual screenshots/logs. Include order ID and test scenario. Exclude
secrets.

## 10. Suggested repository structure

Adapt to the real implementation; do not create empty modules just to
match this example.

``` text
cdc-order-sync/
├── apps/
│   ├── order-service/src/
│   └── cdc-consumer/src/
├── libs/
│   ├── contracts/
│   └── event-normalizer/
├── infrastructure/
│   ├── docker-compose.yml
│   ├── postgres/
│   ├── debezium/
│   └── elasticsearch/
├── scripts/
│   ├── seed-orders.ts
│   ├── reproduce-stale-update.ts
│   ├── replay-event.ts
│   └── reset-local-env.sh
├── test/
│   ├── fixtures/
│   ├── unit/
│   └── integration/
├── docs/
├── deliverables/
├── .env.example
├── .gitignore
├── package.json
└── README.md
```

## 11. Development plan and phase gates

### Phase 0 --- Inspect repository

-   Inspect framework/runtime versions, package manager, Docker
    services, CDC mechanism, event shape, existing tests.
-   Do not overwrite working architecture.
-   **Exit:** architecture summary, assumptions, required changes,
    confirmation docs match actual topology.

### Phase 1 --- Establish baseline

-   Start environment and verify PostgreSQL, CDC transport, consumer,
    Elasticsearch, Kibana.
-   Seed synthetic order and verify normal indexing.
-   Run existing tests and record baseline.
-   **Exit:** environment works and normal sync is demonstrated.

### Phase 2 --- Deterministic defect

-   Implement or confirm one seeded stale-overwrite defect.
-   Add controlled event sequence and reproduction command.
-   Print source/index states.
-   **Exit:** repeatable pre-fix failure, no timing flakiness.

### Phase 3 --- Root-cause analysis

-   Trace event from source through connector, broker, consumer, and
    Elasticsearch.
-   Inspect event IDs, versions, source positions, offsets,
    acknowledgement.
-   **Exit:** root cause supported by code and observable evidence; fix
    strategy written down.

### Phase 4 --- Targeted fix

-   Implement atomic version-aware indexing or the correction justified
    by the actual root cause.
-   Verify duplicate handling, acknowledgement, errors, and tombstone
    semantics.
-   Add structured outcome logs.
-   **Exit:** original reproduction passes; no unsafe read-then-write
    check.

### Phase 5 --- Automated regression

-   Add unit and integration tests for all six scenarios.
-   Model actual broker replay/offset behavior.
-   Ensure isolation and cleanup.
-   **Exit:** all existing/new tests pass repeatably.

### Phase 6 --- Manual validation/evidence

-   Execute six scenarios.
-   Inspect PostgreSQL in pgAdmin and Elasticsearch in Kibana.
-   Capture before/after evidence and record actual outcomes.
-   **Exit:** each matrix row has an observed result and valid evidence.

### Phase 7 --- Handoff

-   Complete spreadsheet/report.
-   Update README setup, reproduce, test, evidence, teardown commands.
-   Check secrets and repository cleanliness.
-   **Exit:** reviewer can reproduce the exercise and artifacts agree
    with code.

## 12. Developer commands

Adapt scripts to the selected package manager and actual services:

``` bash
docker compose up -d
docker compose ps
docker compose logs -f
npm run seed
npm run reproduce:stale
npm run test
npm run test:integration
npm run test:all
docker compose down
```

If connector registration or index initialization is required, automate
it with idempotent scripts and document exact commands. Do not use
`docker compose down -v` as a routine stop command; it removes volumes.
Document a separate destructive reset command.

## 13. Risks and design decisions

1.  Pin compatible Debezium/Kafka/Elasticsearch versions.
2.  Kafka ordering is per partition, not global. Key events by
    `order_id` if relying on partition order, but retain version
    protection.
3.  Use atomic per-order version increments; timestamps alone are
    insufficient.
4.  Preserve delete version/tombstone information to prevent stale
    resurrection.
5.  Commit offsets only after the event outcome is safely handled.
6.  Treat version conflicts as expected stale/duplicate outcomes only
    when confirmed; distinguish them from mapping or infrastructure
    errors.
7.  Test eventual convergence with bounded polling.
8.  Kafka and Elasticsearch consume local resources; document resource
    expectations.
9.  Keep one primary seeded defect and a focused fix.
10. Clearly label controlled replay fixtures versus real broker replay.

## 14. Instructions to the coding LLM

You are acting as a senior backend/distributed-systems engineer building
this local engineering exercise.

1.  Inspect the supplied repository before changing anything.
2.  Confirm the real CDC topology; align implementation and
    documentation with it.
3.  Present an implementation plan and assumptions first.
4.  Preserve existing conventions and architecture.
5.  If building from scratch, create one deterministic stale-overwrite
    defect in the baseline. If the defect already exists, do not add
    another.
6.  Provide one reliable reproduction command.
7.  Fix the root cause using atomic version-aware indexing or another
    justified mechanism. Never use an unsafe read-then-write comparison.
8.  Handle duplicates, restart/replay, and deletion explicitly.
9.  Add tests for all six scenarios. Use real Elasticsearch integration
    tests where claimed.
10. Use synthetic data only. Do not add production credentials or
    unnecessary external dependencies.
11. Make Docker Compose health checks and initialization repeatable.
12. Generate spreadsheet/report templates, but mark unexecuted results
    `NOT RUN`. Never fabricate test results, screenshots, or logs.
13. Run format, lint, typecheck, unit, and integration checks where
    possible. Disclose anything not run and why.
14. Update README setup, reproduction, test, evidence, and teardown
    instructions.
15. Finish with architecture summary, files changed, exact commands,
    explanation of fix, executed test results, unexecuted tests, and
    remaining risks.

**Do not claim completion because code was generated. Run the
environment and verify behavior. Disclose any dependency or test that
could not be executed.**

## 15. Final acceptance checklist

-   [ ] Code and architecture documentation agree.
-   [ ] Docker Compose starts required services.
-   [ ] pgAdmin can inspect PostgreSQL.
-   [ ] Kibana can inspect Elasticsearch.
-   [ ] Normal order synchronization works.
-   [ ] Pre-fix stale-state reproduction is deterministic.
-   [ ] Root cause is documented with evidence.
-   [ ] Fix uses atomic ordering/version protection.
-   [ ] Duplicate delivery is safe.
-   [ ] Consumer restart/replay is honestly tested.
-   [ ] Older events cannot overwrite newer state.
-   [ ] Deletion and stale resurrection are handled.
-   [ ] All six regression scenarios are tested.
-   [ ] Spreadsheet contains actual results and evidence.
-   [ ] Incident report reflects implementation and limitations.
-   [ ] No customer data or secrets are present.
-   [ ] README allows independent reproduction.

**Final principle:** Build the smallest realistic system that makes the
consistency failure observable, the root cause understandable, and the
correction independently verifiable.
