/**
 * Deterministic reproduction of the stale-overwrite defect.
 *
 *   npm run reproduce:stale                 # fresh order ORD-REPRO-<runId>
 *   npm run reproduce:stale -- --out file.json
 *
 * Sequence (no sleeps; every wait is bounded polling or an explicit offset sync):
 *   1. create ORD-REPRO-<runId> (v1 PENDING) and drive it CONFIRMED (v2) -> PROCESSING (v3) -> COMPLETED (v4)
 *   2. wait until Elasticsearch holds v4
 *   3. locate the real Debezium record for v2 in Kafka and re-deliver it (controlled late delivery)
 *   4. wait until the projection consumer group has committed past the re-delivered record
 *   5. compare PostgreSQL and Elasticsearch; exit 1 if they differ
 *
 * Each run uses a new order ID, so it is safe to run repeatedly (order IDs are never re-used).
 */
import { writeFileSync } from 'node:fs';
import {
  api,
  esDoc,
  findOrderMessages,
  pgOrder,
  replayMessage,
  runId as newRunId,
  waitFor,
  waitForCommitted,
  waitForEsVersion,
} from './lib/local-env';

async function main() {
  const runId = newRunId();
  const orderId = `ORD-REPRO-${runId}`;
  const outIndex = process.argv.indexOf('--out');
  const startedAt = new Date().toISOString();
  const step = (msg: string) => console.log(`[reproduce] ${msg}`);

  step(`run ${runId}: order ${orderId}`);
  await api('POST', '/orders', { id: orderId, totalAmount: '149.90', currency: 'EUR' });
  for (const status of ['CONFIRMED', 'PROCESSING', 'COMPLETED']) {
    const o = await api('PATCH', `/orders/${orderId}/status`, { status });
    step(`PATCH status=${status} -> PostgreSQL version ${o.version}`);
  }

  await waitForEsVersion(orderId, 4);
  step('Elasticsearch converged to v4 (COMPLETED) through the normal CDC path');

  const v2 = await waitFor('Debezium record for v2 present in Kafka', async () =>
    (await findOrderMessages(orderId)).find((m) => m.event.version === 2 && m.event.operation === 'UPDATE'),
  );
  step(
    `captured original v2 record: partition=${v2.partition} offset=${v2.offset} lsn=${v2.event.sourcePosition?.lsn} txId=${v2.event.sourcePosition?.transactionId}`,
  );

  const replayed = await replayMessage(v2);
  step(`controlled late delivery: v2 re-published as partition=${replayed.partition} offset=${replayed.offset}`);
  await waitForCommitted(replayed.partition, replayed.offset);
  step('projection consumer committed past the re-delivered record');

  const pg = await pgOrder(orderId);
  const es = await esDoc(orderId);
  const result = {
    runId,
    orderId,
    startedAt,
    finishedAt: new Date().toISOString(),
    replayedOriginal: { partition: v2.partition, offset: v2.offset, version: 2, lsn: v2.event.sourcePosition?.lsn },
    replayedCopy: replayed,
    postgres: pg,
    elasticsearch: es.source
      ? {
          status: es.source.status,
          version: es.source.version,
          is_deleted: es.source.is_deleted,
          last_event_id: es.source.last_event_id,
        }
      : null,
    consistent: !!pg && !!es.source && pg.status === es.source.status && pg.version === es.source.version,
  };

  console.log('');
  console.table({
    PostgreSQL: { status: pg?.status, version: pg?.version },
    Elasticsearch: { status: es.source?.status, version: es.source?.version },
  });
  if (outIndex > 0) writeFileSync(process.argv[outIndex + 1], JSON.stringify(result, null, 2) + '\n');

  if (!result.consistent) {
    console.error(
      `\nFAIL: STALE PROJECTION for ${orderId}: PostgreSQL=${pg?.status}/v${pg?.version}, Elasticsearch=${es.source?.status}/v${es.source?.version}`,
    );
    process.exit(1);
  }
  console.log(
    `\nPASS: ${orderId} consistent (PostgreSQL and Elasticsearch both ${pg?.status}/v${pg?.version}); stale v2 did not regress the projection`,
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(2);
});
