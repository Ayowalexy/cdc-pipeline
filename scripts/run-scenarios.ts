/**
 * Executes the six matrix scenarios (TC-01..TC-06) against the running Docker Compose stack and
 * records observed PostgreSQL / Elasticsearch state. Results are what was observed, nothing else.
 *
 *   npm run scenarios -- --phase before|after
 *
 * Writes deliverables/<phase>/scenario_results.json and deliverables/<phase>/<phase>_consumer_logs.txt
 * (consumer log lines for this run's orders only). Exit code 1 if any scenario failed.
 *
 * TC-03/TC-05/TC-06 use the controlled replay fixture (re-publish a captured Debezium record).
 * TC-04 uses the real broker: the consumer container is stopped, events accumulate as lag, the
 * consumer restarts from its committed offsets; then the group is rewound to earliest (real replay).
 */
import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import {
  api,
  compose,
  consumerLogsSince,
  esDoc,
  esSearchLive,
  findOrderMessages,
  groupLag,
  pgOrder,
  replayMessage,
  resetGroupToEarliest,
  runId as newRunId,
  waitFor,
  waitForCommitted,
  waitForEsVersion,
  waitForZeroLag,
} from './lib/local-env';

type Result = {
  id: string;
  scenario: string;
  orderId: string;
  preconditions: string;
  steps: string[];
  expected: string;
  postgres: unknown;
  elasticsearch: unknown;
  observations: string[];
  pass: boolean;
};

const phase = process.argv[process.argv.indexOf('--phase') + 1];
if (!['before', 'after'].includes(phase)) {
  console.error('usage: npm run scenarios -- --phase before|after');
  process.exit(2);
}
const run = newRunId();
const orderIdFor = (tc: string) => `ORD-${tc}-${run}`;

async function state(orderId: string) {
  const pg = await pgOrder(orderId);
  const es = await esDoc(orderId);
  return {
    postgres: pg ? { status: pg.status, version: pg.version } : 'row absent',
    elasticsearch: es.source
      ? { status: es.source.status, version: es.source.version, is_deleted: es.source.is_deleted, _seq_no: es.seqNo }
      : 'document absent',
    pg,
    es,
  };
}

async function original(orderId: string, version: number, operation = 'UPDATE') {
  return waitFor(`Debezium record ${orderId} v${version} ${operation}`, async () =>
    (await findOrderMessages(orderId)).find((m) => m.event.version === version && m.event.operation === operation),
  );
}

async function replay(orderId: string, version: number, observations: string[]) {
  const msg = await original(orderId, version);
  const copy = await replayMessage(msg);
  await waitForCommitted(copy.partition, copy.offset);
  observations.push(
    `controlled replay of v${version} (original ${msg.partition}/${msg.offset}) as ${copy.partition}/${copy.offset}; consumer committed past it`,
  );
}

async function create(orderId: string) {
  await api('POST', '/orders', { id: orderId, totalAmount: '99.50', currency: 'EUR' });
}
const patch = (orderId: string, status: string) => api('PATCH', `/orders/${orderId}/status`, { status });

const scenarios: Array<() => Promise<Result>> = [
  async () => {
    const orderId = orderIdFor('TC01');
    await create(orderId);
    await waitForEsVersion(orderId, 1);
    await patch(orderId, 'CONFIRMED');
    const observations: string[] = [];
    await waitForEsVersion(orderId, 2).catch(() => observations.push('Elasticsearch did not reach v2 within 60s'));
    const s = await state(orderId);
    return {
      id: 'TC-01',
      scenario: 'Normal update',
      orderId,
      preconditions: 'Stack healthy; new order created (v1 PENDING) and indexed',
      steps: [
        `POST /orders {id:${orderId}}`,
        'wait ES v1',
        `PATCH /orders/${orderId}/status CONFIRMED`,
        'bounded poll ES for v2 (60s)',
      ],
      expected: 'Elasticsearch converges to CONFIRMED / v2',
      postgres: s.postgres,
      elasticsearch: s.elasticsearch,
      observations,
      pass: s.pg?.version === 2 && s.es.source?.version === 2 && s.es.source?.status === 'CONFIRMED',
    };
  },
  async () => {
    const orderId = orderIdFor('TC02');
    await create(orderId);
    const statuses = ['CONFIRMED', 'PROCESSING', 'CONFIRMED', 'PROCESSING', 'COMPLETED'];
    for (const st of statuses) await patch(orderId, st); // back-to-back, no delay
    const observations: string[] = [];
    const pg = await pgOrder(orderId);
    await waitForEsVersion(orderId, pg!.version).catch(() =>
      observations.push(`Elasticsearch did not reach v${pg!.version}`),
    );
    const s = await state(orderId);
    return {
      id: 'TC-02',
      scenario: 'Rapid updates',
      orderId,
      preconditions: 'Stack healthy; new order',
      steps: [
        `POST /orders {id:${orderId}}`,
        `PATCH status ${statuses.join(' -> ')} back-to-back`,
        'bounded poll ES for PostgreSQL version',
      ],
      expected: 'Final indexed version/status matches PostgreSQL (COMPLETED / v6)',
      postgres: s.postgres,
      elasticsearch: s.elasticsearch,
      observations,
      pass: s.pg?.version === 6 && s.es.source?.version === 6 && s.es.source?.status === s.pg?.status,
    };
  },
  async () => {
    const orderId = orderIdFor('TC03');
    await create(orderId);
    await patch(orderId, 'CONFIRMED');
    await waitForEsVersion(orderId, 2);
    const before = await esDoc(orderId);
    const observations: string[] = [`ES _seq_no before duplicates: ${before.seqNo}`];
    await replay(orderId, 2, observations);
    await replay(orderId, 2, observations);
    const s = await state(orderId);
    observations.push(
      `ES _seq_no after duplicates: ${s.es.seqNo} (${s.es.seqNo === before.seqNo ? 'document not rewritten' : 'document rewritten by duplicate'})`,
    );
    return {
      id: 'TC-03',
      scenario: 'Duplicate event',
      orderId,
      preconditions: 'Order at v2 CONFIRMED, indexed',
      steps: [
        `create ${orderId}, PATCH CONFIRMED, wait ES v2`,
        'npm run replay:event -- --order-id <id> --version 2 (twice)',
      ],
      expected: 'No regression; duplicate safely ignored/idempotent (ES stays CONFIRMED / v2, not rewritten)',
      postgres: s.postgres,
      elasticsearch: s.elasticsearch,
      observations,
      pass: s.es.source?.version === 2 && s.es.source?.status === 'CONFIRMED' && s.es.seqNo === before.seqNo,
    };
  },
  async () => {
    const orderId = orderIdFor('TC04');
    const observations: string[] = [];
    await create(orderId);
    await waitForEsVersion(orderId, 1);

    compose('stop', 'cdc-consumer');
    observations.push('cdc-consumer container stopped');
    await patch(orderId, 'CONFIRMED');
    await patch(orderId, 'PROCESSING');
    const pendingLag = await waitFor('lag visible while consumer is stopped', async () => {
      const lag = await groupLag();
      return lag.reduce((sum, p) => sum + p.lag, 0) >= 2 ? lag : undefined; // both updates pending
    });
    observations.push(
      `while stopped: ${JSON.stringify(pendingLag)}; ES version ${(await esDoc(orderId)).source?.version}`,
    );

    compose('up', '-d', '--no-deps', 'cdc-consumer');
    const afterRestart = await waitForZeroLag();
    await waitForEsVersion(orderId, 3);
    observations.push(`after restart: lag 0 ${JSON.stringify(afterRestart)}; ES reached v3`);

    // Real broker replay: rewind the group to earliest (group must be inactive while resetting).
    const seqBeforeReplay = (await esDoc(orderId)).seqNo;
    const since = new Date().toISOString();
    compose('stop', 'cdc-consumer');
    await resetGroupToEarliest();
    observations.push(`group rewound to earliest: ${JSON.stringify(await groupLag())}`);
    compose('up', '-d', '--no-deps', 'cdc-consumer');
    await waitForZeroLag(180_000);
    const replayedLines = consumerLogsSince(since)
      .split('\n')
      .filter((l) => l.includes(`"orderId":"${orderId}"`));
    const outcomes = replayedLines.map((l) => /"outcome":"(\w+)"/.exec(l)?.[1]);
    observations.push(
      `replay re-delivered ${replayedLines.length} events for this order; outcomes: ${outcomes.join(', ')}`,
    );
    const s = await state(orderId);
    observations.push(`ES _seq_no before replay ${seqBeforeReplay}, after ${s.es.seqNo}`);
    return {
      id: 'TC-04',
      scenario: 'Consumer restart/replay',
      orderId,
      preconditions: 'Order indexed at v1; consumer group order-projection has committed offsets',
      steps: [
        'docker compose stop cdc-consumer',
        'PATCH CONFIRMED, PATCH PROCESSING (events pending in Kafka, lag > 0)',
        'docker compose start cdc-consumer; wait lag 0 and ES v3',
        'docker compose stop cdc-consumer; reset group order-projection to earliest; start; wait lag 0',
      ],
      expected: 'Consumer resumes from committed offset; after full replay final state still PROCESSING / v3',
      postgres: s.postgres,
      elasticsearch: s.elasticsearch,
      observations,
      pass:
        replayedLines.length >= 3 &&
        s.pg?.version === 3 &&
        s.es.source?.version === 3 &&
        s.es.source?.status === 'PROCESSING',
    };
  },
  async () => {
    const orderId = orderIdFor('TC05');
    await create(orderId);
    await patch(orderId, 'CONFIRMED');
    await patch(orderId, 'PROCESSING');
    await waitForEsVersion(orderId, 3);
    const observations: string[] = [];
    await replay(orderId, 2, observations);
    const s = await state(orderId);
    return {
      id: 'TC-05',
      scenario: 'Out-of-order event',
      orderId,
      preconditions: 'Order at v3 PROCESSING, indexed',
      steps: [`create ${orderId}, PATCH CONFIRMED, PATCH PROCESSING, wait ES v3`, 'replay v2 (CONFIRMED) after v3'],
      expected: 'Lower version cannot overwrite higher version (ES stays PROCESSING / v3)',
      postgres: s.postgres,
      elasticsearch: s.elasticsearch,
      observations,
      pass: s.es.source?.version === 3 && s.es.source?.status === 'PROCESSING',
    };
  },
  async () => {
    const orderId = orderIdFor('TC06');
    await create(orderId);
    await patch(orderId, 'CONFIRMED');
    await waitForEsVersion(orderId, 2);
    await api('DELETE', `/orders/${orderId}`);
    await waitFor('tombstone indexed', async () => (await esDoc(orderId)).source?.is_deleted === true);
    const observations: string[] = [
      `after delete: tombstone v${(await esDoc(orderId)).source?.version}, live search hits ${await esSearchLive(orderId)}`,
    ];
    await replay(orderId, 2, observations);
    const s = await state(orderId);
    const hits = await esSearchLive(orderId);
    observations.push(`live search hits after stale update: ${hits}`);
    return {
      id: 'TC-06',
      scenario: 'Deletion',
      orderId,
      preconditions: 'Order at v2 CONFIRMED, indexed',
      steps: [
        `create ${orderId}, PATCH CONFIRMED, wait ES v2`,
        `DELETE /orders/${orderId}; wait tombstone`,
        'replay v2 (stale update)',
        'live search (is_deleted:false)',
      ],
      expected: 'Order remains deleted (tombstone v3) and not searchable',
      postgres: s.postgres,
      elasticsearch: s.elasticsearch,
      observations,
      pass: s.pg === null && s.es.source?.is_deleted === true && s.es.source?.version === 3 && hits === 0,
    };
  },
];

async function main() {
  const startedAt = new Date().toISOString();
  const outDir = join(__dirname, '..', 'deliverables', phase);
  mkdirSync(outDir, { recursive: true });
  const results: Result[] = [];
  for (const scenario of scenarios) {
    const r = await scenario().catch(
      (err): Result => ({
        id: `TC-0${results.length + 1}`,
        scenario: 'error',
        orderId: '',
        preconditions: '',
        steps: [],
        expected: '',
        postgres: null,
        elasticsearch: null,
        observations: [`scenario threw: ${err instanceof Error ? err.message : String(err)}`],
        pass: false,
      }),
    );
    compose('up', '-d', '--no-deps', 'cdc-consumer'); // never leave the consumer stopped after a scenario error
    results.push(r);
    console.log(
      `${r.id} ${r.pass ? 'PASS' : 'FAIL'}  ${r.scenario}  PG=${JSON.stringify(r.postgres)} ES=${JSON.stringify(r.elasticsearch)}`,
    );
    for (const o of r.observations) console.log(`        ${o}`);
  }
  const finishedAt = new Date().toISOString();
  writeFileSync(
    join(outDir, 'scenario_results.json'),
    JSON.stringify({ phase, runId: run, startedAt, finishedAt, results }, null, 2) + '\n',
  );
  const ids = results.map((r) => r.orderId).filter(Boolean);
  const logs = consumerLogsSince(startedAt)
    .split('\n')
    .filter((l) => ids.some((id) => l.includes(id)));
  writeFileSync(join(outDir, `${phase}_scenarios_consumer_logs.txt`), logs.join('\n') + '\n');
  console.log(
    `\nrun ${run}: ${results.filter((r) => r.pass).length}/${results.length} passed -> deliverables/${phase}/scenario_results.json`,
  );
  process.exit(results.every((r) => r.pass) ? 0 : 1);
}

void main();
