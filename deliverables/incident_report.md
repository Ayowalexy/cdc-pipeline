# Incident Report: Stale order state in the Elasticsearch projection

| | |
| --- | --- |
| System | CDC Order Synchronization (local exercise): PostgreSQL → Debezium → Kafka → NestJS consumer → Elasticsearch |
| Defect | An older order event overwrites a newer Elasticsearch document |
| Environment | Docker Compose on one developer machine (macOS, Intel i5, Docker Desktop with 8 vCPU and 11.7 GB) |
| Data | Synthetic orders only |
| Status | Fixed and validated locally. Not deployed anywhere else. |
| Evidence | `deliverables/before/`, `deliverables/after/`, `deliverables/test_matrix.xlsx` (the Evidence Index sheet maps every file listed here) |

## 1. Executive summary

The search projection could regress to an older order state while PostgreSQL stayed correct. When an older change event for an order reached the consumer after a newer one (a re-delivery, a replay or an out-of-order arrival), the consumer wrote it unconditionally. Elasticsearch then showed, for example, `CONFIRMED / v2` for an order that PostgreSQL held as `COMPLETED / v4`. The same flaw let a stale update resurrect a deleted order, which then became searchable again.

The root cause was an unconditional `index` call in `OrderProjectionService.apply`, with no version guard. The fix uses Elasticsearch external versioning, keyed on the PostgreSQL row version. The comparison and the write happen atomically in the shard: newer events apply, equal versions are reported as duplicates, and older versions are rejected. Deletes write versioned tombstones.

After the fix, the deterministic reproduction, all six regression scenarios (TC-01 to TC-06), 38 automated tests (unit plus real Elasticsearch and Kafka integration) and a global PostgreSQL↔Elasticsearch convergence check all pass.

Validation also found one deployment hazard: documents written by the old consumer must be rebuilt once before the fix behaves correctly for them (section 9).

## 2. Architecture and data flow

```text
order-service ──SQL──► PostgreSQL 16 ──logical replication (pgoutput)──► Debezium 2.7.3 (Kafka Connect)
   (NestJS)            orders: version = version + 1 per status change,      topic.prefix=cdc, JSON, no schemas
                       REPLICA IDENTITY FULL                                  │
                                                                              ▼
Elasticsearch 8.15 ◄──index──── cdc-consumer (NestJS, kafkajs) ◄────── Kafka 3.8 KRaft: cdc.public.orders
 index `orders`                  Debezium adapter → OrderChangeEvent           3 partitions, key {"id": <order id>}
 _id = order id                  projection → structured outcome log           + cdc.public.orders.dlq
```

- **PostgreSQL is authoritative.** `PATCH /orders/:id/status` runs a single `UPDATE … SET version = version + 1 … WHERE id = $1 AND status <> $2`. The row lock makes the increment atomic, and a no-op status change does not bump the version.
- **Debezium** emits `c`, `u`, `d` and `r` (snapshot) envelopes keyed by primary key. All events for one order therefore land in one partition, in commit order.
- **The adapter** ([libs/event-normalizer/debezium-order.adapter.ts](../libs/event-normalizer/debezium-order.adapter.ts)) is the only code that knows the Debezium envelope. It produces `{ eventId, aggregateId, operation, version, occurredAt, sourcePosition: { lsn, transactionId, partition, offset }, after, before }`. A delete gets `version = before.version + 1`; order IDs are never re-used.
- **The consumer** commits offsets manually, after each event's outcome is established, and logs one JSON line per event.

## 3. Observed symptom

For reproduction order `ORD-REPRO-20261103-065337` (pre-fix, 2026-11-03T06:53Z):

| Store | status | version | Evidence |
| --- | --- | --- | --- |
| PostgreSQL (pgAdmin) | `COMPLETED` | 4 | `before/before_postgres.png`, `before/before_postgres.txt` |
| Elasticsearch (Kibana Dev Tools `GET orders/_doc/…`) | `CONFIRMED` | 2 | `before/before_elasticsearch.png`, `before/before_elasticsearch.txt` |

Related symptoms observed in the pre-fix scenario run (`before/scenario_results.json`, run `20261103-070441`):

- **TC-05:** the order was at `PROCESSING / v3`, and a late v2 regressed Elasticsearch to `CONFIRMED / v2`.
- **TC-06:** the order was deleted in PostgreSQL, but a stale v2 replaced the tombstone with a live `CONFIRMED / v2` document. The application search query (`is_deleted: false`) returned it: 1 hit, where 0 was expected.
- **TC-03:** each duplicate re-wrote the document (`_seq_no` 57 → 59) and was logged as `applied`, so duplicates could not be told apart from real changes.

Normal flow was unaffected. TC-01 and TC-02 passed before the fix, and in-order delivery converged.

## 4. Reproduction steps

```bash
docker compose up -d --build --wait
npm run reproduce:stale
```

1. Create `ORD-REPRO-<runId>` (v1 `PENDING`), then `PATCH` it to `CONFIRMED` (v2), `PROCESSING` (v3) and `COMPLETED` (v4).
2. Poll (bounded to 60 s) until Elasticsearch holds v4.
3. Find the original Debezium record for v2 in `cdc.public.orders` and re-publish its exact bytes, with the same key, to the same partition, using the headers `x-replay: controlled` and `x-replay-of`. This is a **controlled replay fixture**. It models redelivery; it does not rewind the broker.
4. Wait until the `order-projection` group has committed past the re-published offset. This is an explicit synchronisation on the offset; there are no sleeps.
5. Compare PostgreSQL and Elasticsearch. A mismatch exits 1.

The result was deterministic. Before the fix, 3 of 3 runs failed with exit 1 and showed `PostgreSQL=COMPLETED/v4, Elasticsearch=CONFIRMED/v2` (`before/before_reproduce_output.txt`). After the fix, 3 of 3 runs passed with exit 0.

## 5. Root cause with code and event evidence

**Event trace** for the order, from the pre-fix consumer log (`before/before_consumer_logs.txt`):

| Kafka position | Event | Outcome logged |
| --- | --- | --- |
| `cdc.public.orders/1/8` | v1 CREATE (txId 751) | applied |
| `…/1/9` | v2 UPDATE, LSN 26327128, txId 752 | applied |
| `…/1/10` | v3 UPDATE | applied |
| `…/1/11` | v4 UPDATE | applied |
| `…/1/12` | **v2 UPDATE**, the same LSN 26327128 and txId 752, `replay: controlled` | **applied** |

```json
{"msg":"order event","eventId":"order:ORD-REPRO-20261103-065337:v2:UPDATE","operation":"UPDATE","incomingVersion":2,
 "sourcePosition":{"transactionId":"752","lsn":"26327128","partition":1,"offset":"12"},
 "correlationId":"cdc.public.orders/1/12","replay":"controlled","outcome":"applied"}
```

The connector, the broker and the adapter all behaved correctly. The record at offset 12 is a faithful copy of the v2 change: the same LSN, the same transaction and the same `eventId`. Kafka delivered it in partition order, exactly as it should. The incoming event carried enough information (`version: 2`) to see that it was stale, and nothing used it.

**Code** (baseline `apps/cdc-consumer/src/order-projection.service.ts`, the `initial` commit):

```ts
await this.client.index({ index: this.index, id: event.aggregateId, document });
return { outcome: event.operation === 'DELETE' ? 'deleted' : 'applied' };
```

`index` without a version condition is a last-write-wins overwrite keyed only by document ID. Kafka guarantees order only within a partition, and only for records as they were appended. A redelivered, replayed or re-sent record appears *later* in the log than newer changes. Partition ordering therefore cannot stop a stale write; only a comparison of versions at write time can. Pre-fix, the document's `_version` was 10 while the row version was 4: Elasticsearch had counted every write, including the stale ones.

The same missing guard explains the other symptoms:

- **Resurrection (TC-06):** the tombstone was overwritten by an older update.
- **Duplicates rewriting the document (TC-03):** an equal version is not detected.
- **The concurrent-write integration test:** five versions written concurrently ended at v3, not v5.

## 6. Fix and design rationale

**Change** (the whole diff is [docs/fix.diff](../docs/fix.diff), in one file):

```ts
await this.client.index({
  index: this.index, id: event.aggregateId, document,
  version: event.version, version_type: 'external',
});
// 409 version_conflict_engine_exception -> { outcome: 'duplicate' | 'ignored_stale', indexedVersion }
```

- **Atomic compare-and-write.** With `version_type: external`, the shard accepts a write only if the incoming version is strictly greater than the stored one, and it checks and writes under the document's lock. Equal versions are rejected, which gives the "duplicate is a no-op" rule; lower versions are rejected, which gives "stale never overwrites". No read-then-write exists in application code.
- **Why external versioning rather than a scripted update.** The consumer always holds a complete snapshot of the row (`after`, or `before` for a delete). The order version is a positive, monotonically increasing integer that PostgreSQL assigns. That is exactly the contract external versioning needs. A Painless script would do the same comparison with more code and a script to maintain, and it would gain nothing here.
- **Conflict classification.** Only `409` with `version_conflict_engine_exception` is an expected outcome. The stored version is read from the conflict reason (`current version [N]`) to log `indexedVersion`, and to tell a `duplicate` (N equals the incoming version) from an `ignored_stale` (N is greater). Every other error (mapping, 4xx, 5xx, connection) still goes to the existing retryable or permanent handling.
- **Deletion.** A delete writes a tombstone document (`is_deleted: true`, the last known fields, `deleted_at`) with external version `before.version + 1`. Stale updates for the order are then rejected forever, so the order cannot be resurrected. Application searches filter on `is_deleted: false`.
- **Tombstone retention.** Tombstones are never cleaned up in this exercise. A cleanup job may delete a tombstone only after the replay horizon has passed, meaning no older record for that order can still be delivered or replayed. This is not implemented. Locally the topic has infinite retention, so tombstones must stay.
- **Idempotency and offsets** were already in place in the baseline and are unchanged. The consumer is at-least-once: it commits after the outcome is known, retries transient errors in place, sends permanent errors to the DLQ, and relies on redelivery after a crash. The fix makes redelivery safe, because a re-applied event now results in `duplicate` or `ignored_stale` and never in a regression. Exactly-once delivery is not claimed.

## 7. Validation summary (see `test_matrix.xlsx`)

| ID | Scenario | Pre-fix (run 20261103-070441) | Post-fix (run 20261103-073747) |
| --- | --- | --- | --- |
| TC-01 | Normal update | PASS | PASS: `CONFIRMED / v2` in both stores |
| TC-02 | Rapid updates (5 back-to-back) | PASS | PASS: `COMPLETED / v6` in both stores |
| TC-03 | Duplicate event (v2 delivered twice more) | FAIL: rewritten, `_seq_no` 57 → 59 | PASS: `duplicate`, `_seq_no` unchanged |
| TC-04 | Consumer restart and **real broker replay** | PASS on final state; replay logged `applied` ×3 and rewrote documents | PASS: lag accumulated while stopped, resumed from the committed offset, then the group was rewound to earliest; this order logged `ignored_stale`, `ignored_stale`, `duplicate`, `_seq_no` unchanged |
| TC-05 | Out-of-order (v2 after v3) | FAIL: `CONFIRMED / v2` | PASS: `PROCESSING / v3` |
| TC-06 | Delete, then stale v2 | FAIL: resurrected, 1 search hit | PASS: tombstone v3, 0 search hits |
| REG-01 | Seeded-defect reproduction | FAIL (3/3 runs) | PASS (3/3 runs) |
| AUTO-01 | `npm run test:all` | integration: 9 failed, 6 passed | 38/38 passed (23 unit, 15 integration) |
| CONV-01 | Global convergence | n/a | 20 mismatches before the rebuild; 0 after; 0 after the final run (34 rows, 3 tombstones) |

What TC-04 proves: while the consumer container was stopped, the group's committed offset stayed behind the log end (lag 2). After the restart it consumed exactly the pending records, and lag returned to 0. The group was then reset to earliest with the consumer stopped and the group `Empty`, and the restarted consumer re-read the entire topic. The integration test `consumer-recovery.kafka.spec.ts` asserts the exact offsets re-read in each case against the real broker. It also covers a crash between write and commit (redelivered, logged `duplicate`) and exhausted retries (offset not committed, redelivered after restart).

Static checks: `npm run typecheck`, `npm run lint` and `npm run format:check` are clean.

## 8. Evidence filenames

| Before (seeded defect) | After (fix) |
| --- | --- |
| `before/before_postgres.png`: pgAdmin, `ORD-REPRO-20261103-065337` COMPLETED/4 | `after/after_postgres.png`: pgAdmin, `ORD-REPRO-20261103-073741` COMPLETED/4 |
| `before/before_elasticsearch.png`: Kibana, same order CONFIRMED/2, `_version` 10 | `after/after_elasticsearch.png`: Kibana, same order COMPLETED/4, `_version` 4 |
| `before/before_consumer_logs.txt` | `after/after_consumer_logs.txt` (replayed v2 `ignored_stale`, plus the TC-04 rewind of the same offsets) |
| `before/before_reproduce_output.txt`, `before_reproduce_result.json` | `after/after_reproduce_output.txt`, `after_reproduce_result.json` |
| `before/before_postgres.txt`, `before_elasticsearch.txt` | `after/after_postgres.txt`, `after_elasticsearch.txt` |
| `before/before_integration_tests.txt` | `after/after_all_tests.txt`, `after_integration_tests.txt` |
| `before/scenario_results.json`, `before_scenarios_output.txt`, `before_scenarios_consumer_logs.txt` | `after/scenario_results.json`, `after_scenarios_output.txt`, `after_scenarios_consumer_logs.txt` |
| | `after/after_prefix_document_hazard_logs.txt`, `convergence_before_rebuild.json`, `after_rebuild_output.txt`, `convergence_after_rebuild.json`, `convergence_final.json` |

The screenshots are of the real pgAdmin 8.12 and Kibana 8.15.3 UIs, driven by headless Chrome. The caption overlay was added by the capture script. The narrated screen recording (PRD section 8) was **not** made; it needs a human presenter.

## 9. Remaining risks and limitations

**Found during validation and handled:**

- **Deployment hazard (fixed through a procedure, not code).** Documents written by the old consumer carry Elasticsearch's internal `_version`, which counts writes. External versioning compares against it. After the fix, a legitimate v5 update to `ORD-REPRO-20261103-065337` (internal `_version` 10) was rejected as `ignored_stale` with `indexedVersion: 10` (`after/after_prefix_document_hazard_logs.txt`). A global check found 20 mismatching pre-fix documents. `npm run rebuild:projection` rebuilt the index from the CDC log, and the result was 0 mismatches. **Any deployment of this fix onto an existing index must include the rebuild.** The local rebuild depends on infinite topic retention. In production the source should be a fresh Debezium snapshot or a re-index from PostgreSQL, not the Kafka log.
- **Operational issues fixed while making TC-04 reliable.** None of these is the seeded defect.
  - Kafka's healthcheck started a JVM on every probe; on the loaded host the probes piled up and starved the broker. It was replaced with a plain TCP check. A first version that also grepped `server.log` went unhealthy once the broker rotated that log, which it does hourly.
  - The busybox `wget localhost` resolved to IPv6 while the apps listened on IPv4, so healthchecks failed. They now use `127.0.0.1`.
  - With Node as PID 1 and an effective grace period of about 1 s, the consumer was SIGKILLed before it left the group, which blocked offset resets. Fixed with `init: true`, `stop_grace_period: 20s` and stopping the consumer in `beforeApplicationShutdown`.
  - `kafka-init` topic creation timed out under load. It now retries.

**Not addressed or untested:**

- Tombstones are never cleaned up, and no retention job exists.
- Re-creating an order with a previously deleted ID would be rejected as stale. The API does not prevent ID reuse; it relies on unique IDs.
- Duplicate-versus-stale classification parses the Elasticsearch conflict reason string. This is verified against 8.15.3 only, and it affects logging only, not safety.
- The system is single-node with no replication; there is no security (TLS, authentication); and it has not been load-tested or benchmarked.
- There is no tooling to reprocess DLQ messages.
- kafkajs 2.2.4 is no longer actively maintained, and on Node 24 it prints a harmless `TimeoutNegativeWarning`.
- Debezium's initial snapshot coalesces history. Under heavy load, one early order was first seen through a snapshot read (`op=r`) at its current version. That is correct, but intermediate versions are not emitted for it.
- Consumer `/health` reported Elasticsearch `down` when pings timed out on the overloaded host, although Elasticsearch was green. It does not distinguish a slow ping from an outage.
- Rapid updates were tested as back-to-back sequential HTTP calls (TC-02) and as concurrent Elasticsearch writes (integration). Concurrent API requests against one order were not load-tested.

## 10. Conclusion and deployment recommendation

The defect is understood, reproduced deterministically, and fixed by one targeted change at the point where the projection is written. In every tested scenario, including duplicates, out-of-order delivery, restarts, a full broker replay, deletion and stale resurrection, the projection converges to PostgreSQL and never regresses.

These are local results only, on a single-node stack, so they do not establish production readiness. Before any shared environment:

1. Deploy the fix together with a one-time projection rebuild, then confirm with `verify:convergence`.
2. Define and implement tombstone retention against the real replay horizon.
3. Add alerting on `permanent_error`, DLQ depth and consumer lag.
4. Re-run the regression suite against the target Elasticsearch and Debezium versions.
