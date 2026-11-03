# CDC Order Synchronization

A local engineering exercise. PostgreSQL holds the orders (it is the source of truth). Elasticsearch holds a search projection of them that is kept up to date asynchronously through a real CDC pipeline. The exercise seeds one consistency defect (an older event overwrites a newer document), reproduces it deterministically, fixes it with atomic version-aware indexing and validates six scenarios.

All data is synthetic. Security is disabled on purpose, because this stack is local-only.

## Architecture

```text
 order-service (NestJS)          Debezium 2.7 PostgreSQL connector        cdc-consumer (NestJS)
 POST/PATCH/DELETE /orders  ──►  PostgreSQL 16 ──WAL (pgoutput)──►  Kafka 3.8 (KRaft)  ──►  kafkajs, manual commits  ──►  Elasticsearch 8.15
 version = version + 1           orders, REPLICA IDENTITY FULL       topic cdc.public.orders       normalize (adapter)           index `orders`
                                                                     3 partitions, key = order id  version-guarded index         doc id = order id
                pgAdmin ─┘ (inspect source)                                                                Kibana ─┘ (inspect projection)
```

| Component         | Where                                                                                                                                                                                  |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Order API         | [apps/order-service](apps/order-service/src): `POST /orders`, `GET /orders/:id`, `PATCH /orders/:id/status`, `DELETE /orders/:id`, `GET /health`                                       |
| Schema            | [infrastructure/postgres/init/001-schema.sql](infrastructure/postgres/init/001-schema.sql): `version` is bumped atomically in a single `UPDATE`                                          |
| Connector config  | [infrastructure/debezium/orders-connector.json](infrastructure/debezium/orders-connector.json), registered by the `connect-init` service with [scripts/register-connector.sh](scripts/register-connector.sh) (idempotent `PUT`) |
| Debezium adapter  | [libs/event-normalizer/debezium-order.adapter.ts](libs/event-normalizer/debezium-order.adapter.ts): the only code that knows the Debezium envelope; it emits `OrderChangeEvent` ([libs/contracts](libs/contracts/order-change-event.ts)) |
| Consumer          | [apps/cdc-consumer/src/order-events.consumer.ts](apps/cdc-consumer/src/order-events.consumer.ts): offsets, retries, DLQ, structured outcome logs                                       |
| Projection        | [apps/cdc-consumer/src/order-projection.service.ts](apps/cdc-consumer/src/order-projection.service.ts): version-guarded writes and tombstones                                           |
| Index mapping     | [infrastructure/elasticsearch/orders-index.json](infrastructure/elasticsearch/orders-index.json) (`dynamic: strict`), created by the consumer on startup when it is missing              |

### Images (pinned)

`postgres:16.4`, `apache/kafka:3.8.0` (single-node KRaft, no ZooKeeper), `quay.io/debezium/connect:2.7.3.Final`, `docker.elastic.co/elasticsearch/elasticsearch:8.15.3`, `docker.elastic.co/kibana/kibana:8.15.3`, `dpage/pgadmin4:8.12`, `node:22.11-alpine` (the apps).

### Host ports

| Service                   | URL / port                                     |
| ------------------------- | ---------------------------------------------- |
| order-service             | http://localhost:3100                          |
| cdc-consumer health       | http://localhost:3101/health                   |
| PostgreSQL                | localhost:55432 (`orders` / `orders_local_only`) |
| pgAdmin                   | http://localhost:5050 (no login; the server password is `orders_local_only`) |
| Kafka (host listener)     | localhost:29092                                |
| Kafka Connect REST        | http://localhost:8083                          |
| Elasticsearch             | http://localhost:9200                          |
| Kibana                    | http://localhost:5601                          |

The ports differ from the defaults (5432, 9092, …) to avoid clashing with other local stacks. They can be changed in `.env`.

### Resource expectations

The stack runs 8 long-lived containers, 4 of them JVMs. Give Docker at least 4 CPUs and 6 GB of memory. On a heavily loaded host, startup can take several minutes. The healthchecks were written so that they do not add load (see the [incident report](deliverables/incident_report.md), section 9).

## Setup

Prerequisites: Docker Desktop with Compose v2, Node.js 20 or later, and npm.

```bash
cp .env.example .env                 # optional: compose falls back to the same defaults
npm ci
docker compose up -d --build --wait  # builds the app image; waits for every healthcheck
docker compose ps                    # kafka-init and connect-init show "exited (0)": they are one-shot
curl -s localhost:8083/connectors/orders-connector/status   # connector and task 0 RUNNING
npm run seed                         # ORD-1001..ORD-1005 and waits until they are indexed
```

Startup order is enforced with healthchecks: postgres, kafka, then `kafka-init` (creates `cdc.public.orders` with 3 partitions, the DLQ topic and infinite retention), then connect, then `connect-init` (registers the connector), then cdc-consumer. Broker topic auto-creation is disabled, so the consumer cannot accidentally create a 1-partition topic.

## Inspecting state

**pgAdmin** (http://localhost:5050): open the server **cdc-orders (local)**, enter the password `orders_local_only`, then open the Query Tool:

```sql
SELECT id, status, version, updated_at FROM orders WHERE id = 'ORD-1001';
```

**Kibana Dev Tools** (http://localhost:5601/app/dev_tools#/console):

```http
GET orders/_doc/ORD-1001

# application search: live (non-deleted) orders only
GET orders/_search
{ "query": { "bool": { "filter": [ { "term": { "status": "CONFIRMED" } }, { "term": { "is_deleted": false } } ] } } }

# verify that deleted orders are excluded (expect 0 hits for a deleted id)
GET orders/_search
{ "query": { "bool": { "filter": [ { "term": { "order_id": "ORD-TC06-..." } }, { "term": { "is_deleted": false } } ] } } }

# tombstones (kept for stale-event protection)
GET orders/_search
{ "query": { "term": { "is_deleted": true } } }
```

**Consumer logs**: there is one JSON line per event. The fields are `eventId`, `orderId`, `operation`, `incomingVersion`, `indexedVersion` (on conflicts), `sourcePosition` (txId, LSN, partition, offset), `correlationId` (topic/partition/offset), `replay` and `outcome` (`applied`, `ignored_stale`, `duplicate`, `deleted`, `retryable_error` or `permanent_error`).

```bash
docker compose logs -f cdc-consumer
docker compose logs --no-log-prefix cdc-consumer | grep ORD-1001
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-projection
```

## Reproducing the stale-update defect

```bash
npm run reproduce:stale
```

[scripts/reproduce-stale-update.ts](scripts/reproduce-stale-update.ts) runs these steps:

1. Creates a new order `ORD-REPRO-<runId>`. Every run uses a fresh order, so the script is safe to run repeatedly.
2. Drives it through `CONFIRMED` (v2), `PROCESSING` (v3) and `COMPLETED` (v4) through the API.
3. Waits, by bounded polling, until Elasticsearch shows v4.
4. Finds the real Debezium record for v2 in Kafka and re-publishes the same bytes, with the same key, to the same partition. The copy carries the headers `x-replay: controlled` and `x-replay-of: <partition>/<offset>`. This is a controlled replay fixture, not a broker rewind. It models a redelivery or an upstream re-send.
5. Waits until the consumer group has committed past the re-published record. This is an explicit offset sync, with no sleeps.
6. Prints both states and exits `1` if they differ.

| Code                                    | Output                                                                   | Exit |
| --------------------------------------- | ------------------------------------------------------------------------ | ---- |
| Before the fix (unconditional `index`)  | `FAIL: STALE PROJECTION … PostgreSQL=COMPLETED/v4, Elasticsearch=CONFIRMED/v2` | 1    |
| After the fix (external versioning)     | `PASS: … consistent (… COMPLETED/v4); stale v2 did not regress the projection` | 0    |

To see the pre-fix behaviour, temporarily reverse the fix, then rebuild the consumer:

```bash
patch -p1 -R < docs/fix.diff        # restore the seeded defect (or: git stash, if the fix is uncommitted)
docker compose up -d --build --wait cdc-consumer
npm run reproduce:stale             # FAIL, exit 1
patch -p1 < docs/fix.diff           # re-apply the fix
docker compose up -d --build --wait cdc-consumer
npm run rebuild:projection          # required after running the defective consumer (see below)
```

Any re-delivery can be triggered by hand:

```bash
npm run replay:event -- --order-id ORD-1003 --version 2 [--times 2]
```

## The fix

The projection writes with `version: <PostgreSQL row version>, version_type: 'external'`. Elasticsearch performs the comparison and the write atomically inside the shard:

| Incoming vs. stored version | Result                                    | Logged as       |
| --------------------------- | ----------------------------------------- | --------------- |
| higher                      | written                                   | `applied` / `deleted` |
| equal                       | HTTP 409, not written (`_seq_no` unchanged) | `duplicate`     |
| lower                       | HTTP 409, not written                     | `ignored_stale` |

There is no read-then-write in application code. Only `409 version_conflict_engine_exception` is treated as an expected outcome; mapping and infrastructure errors still propagate. Deletes write a **versioned tombstone** (`is_deleted: true`, version = last row version + 1). Older updates therefore cannot resurrect a deleted order, and application searches filter on `is_deleted: false`. The full diff is in [docs/fix.diff](docs/fix.diff). The rationale is in the [incident report](deliverables/incident_report.md).

### Deploying the fix onto an existing index

Documents written by the old consumer carry Elasticsearch's *internal* `_version`, which counts writes and is not the row version. External versioning compares against that counter. Without a rebuild, legitimate newer updates to those documents are rejected as stale; this was observed directly (see the incident report). So after deploying the fix, rebuild the projection once:

```bash
npm run rebuild:projection   # stop consumer, delete index, rewind group to earliest, start, verify convergence
npm run verify:convergence   # every PostgreSQL row matches a live doc; every other doc is a tombstone
```

## Delivery semantics

The pipeline is **at-least-once**, made safe by idempotent writes. It is not exactly-once.

- **Commit timing.** `autoCommit` is off. An offset is committed only after the outcome of its event is established: `applied`, `deleted`, `duplicate` or `ignored_stale`, or the record has been written to the DLQ.
- **Transient errors** (connection failures, timeouts, 408/429/5xx). These are retried in place with exponential backoff, capped at 5 s, for 8 attempts, with heartbeats in between. Retrying in place blocks the partition, which preserves per-order order. If the attempts run out, the offset is not committed and the consumer restarts, so the event is re-read from the last committed offset.
- **Permanent errors** (unparseable record, unknown op or status, 4xx from Elasticsearch). The original record is sent to `cdc.public.orders.dlq` with source topic, partition, offset and error headers. It is logged as `permanent_error` and its offset is committed.
- **Version conflicts.** These are expected, logged outcomes, not errors.
- **Process failure.** If the process crashes after the Elasticsearch write but before the commit, the event is redelivered on restart, and the version guard turns it into a `duplicate`. On graceful shutdown the consumer leaves the group (`init: true` plus `stop_grace_period`), so it can be restarted or its offsets reset immediately.
- **Ordering.** Kafka orders events only within a partition. Debezium keys records by order id, so all events for one order share a partition. The version guard still protects against redelivery, replay and any out-of-order arrival.

## Tests

```bash
npm test                  # unit: Debezium adapter, error classification, version decision (no Docker needed)
npm run test:integration  # needs the compose stack: real Elasticsearch (:9200) and real Kafka (:29092)
npm run test:all
npm run scenarios -- --phase after   # TC-01..TC-06 against the running pipeline; writes deliverables/after/
npm run typecheck && npm run lint && npm run format:check
```

The integration tests use their own index, topic and consumer group for each run, and clean them up afterwards. They never touch the running pipeline. `consumer-recovery.kafka.spec.ts` drives the real consumer class against the broker. It checks four behaviours: resume from the committed offset after a stop, no commit when retries are exhausted, idempotency when a crash falls between write and commit, and a rewind of the group to earliest.

`npm run scenarios` operates on the live stack. TC-04 stops and starts the `cdc-consumer` container and rewinds the `order-projection` group to earliest. That is a real broker replay of the whole topic.

## Evidence and deliverables

- [deliverables/incident_report.md](deliverables/incident_report.md): the incident report.
- [deliverables/test_matrix.xlsx](deliverables/test_matrix.xlsx): test results and evidence index. Regenerate it from the recorded result files with `python3 scripts/build-test-matrix.py .` (needs `openpyxl`).
- `deliverables/before/` and `deliverables/after/`: screenshots of the real pgAdmin and Kibana UIs, consumer logs, script outputs, machine-readable results and convergence reports.

## Teardown

```bash
docker compose stop        # stop, keep data
docker compose down        # remove containers, keep volumes (PostgreSQL data, Kafka log, Elasticsearch index)
npm run reset:local        # DESTRUCTIVE: down -v (deletes all volumes), then a fresh up --wait; asks for confirmation
```

## Repository layout

```text
apps/order-service/       NestJS order API (PostgreSQL via pg)
apps/cdc-consumer/        NestJS Kafka consumer + Elasticsearch projection
libs/contracts/           OrderChangeEvent, OrderDocument, outcomes
libs/event-normalizer/    Debezium envelope -> OrderChangeEvent adapter
infrastructure/           postgres init SQL, Debezium connector config, ES mapping, pgAdmin server list
scripts/                  seed, reproduce, replay, scenarios, verify/rebuild projection, connector registration, reset
test/unit, test/integration, test/fixtures
docs/fix.diff             the targeted fix
deliverables/             evidence, test matrix, incident report
```
