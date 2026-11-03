/**
 * Compares the whole projection with PostgreSQL:
 *   - every PostgreSQL row has a live Elasticsearch document with the same status and version
 *   - every Elasticsearch document without a PostgreSQL row is a tombstone (is_deleted: true)
 *   - every document's internal _version equals its row version (external versioning in effect)
 *
 *   npm run verify:convergence            # waits for consumer lag 0 first
 *   npm run verify:convergence -- --out deliverables/after/convergence.json
 */
import { writeFileSync } from 'node:fs';
import { Client as PgClient } from 'pg';
import { OrderDocument } from '../libs/contracts/order-change-event';
import { env, waitForZeroLag } from './lib/local-env';

async function main() {
  await waitForZeroLag(300_000);

  const pg = new PgClient(env.pg);
  await pg.connect();
  const { rows } = await pg.query<{ id: string; status: string; version: number }>(
    'SELECT id, status, version FROM orders',
  );
  await pg.end();

  await fetch(`${env.esUrl}/${env.esIndex}/_refresh`, { method: 'POST' });
  const res = await fetch(`${env.esUrl}/${env.esIndex}/_search?version=true`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ size: 10_000, query: { match_all: {} } }),
  });
  const body = (await res.json()) as { hits: { hits: { _id: string; _version: number; _source: OrderDocument }[] } };
  const docs = new Map(body.hits.hits.map((h) => [h._id, h]));

  const mismatches: string[] = [];
  for (const row of rows) {
    const doc = docs.get(row.id);
    if (!doc) mismatches.push(`${row.id}: missing in Elasticsearch (PostgreSQL ${row.status}/v${row.version})`);
    else if (doc._source.is_deleted || doc._source.status !== row.status || doc._source.version !== row.version)
      mismatches.push(
        `${row.id}: PostgreSQL ${row.status}/v${row.version} vs Elasticsearch ${doc._source.status}/v${doc._source.version}${doc._source.is_deleted ? ' (tombstone)' : ''}`,
      );
    else if (doc._version !== row.version)
      mismatches.push(
        `${row.id}: Elasticsearch _version ${doc._version} != row version ${row.version} (pre-fix document)`,
      );
    docs.delete(row.id);
  }
  for (const [id, doc] of docs) {
    if (!doc._source.is_deleted) mismatches.push(`${id}: live in Elasticsearch but absent from PostgreSQL`);
  }

  const summary = {
    checkedAt: new Date().toISOString(),
    postgresRows: rows.length,
    tombstones: [...docs.values()].filter((d) => d._source.is_deleted).length,
    mismatches,
    converged: mismatches.length === 0,
  };
  const out = process.argv.indexOf('--out');
  if (out > 0) writeFileSync(process.argv[out + 1], JSON.stringify(summary, null, 2) + '\n');
  console.log(
    `PostgreSQL rows: ${summary.postgresRows}, tombstones: ${summary.tombstones}, mismatches: ${mismatches.length}`,
  );
  for (const m of mismatches) console.log(`  ${m}`);
  console.log(summary.converged ? 'CONVERGED' : 'NOT CONVERGED');
  process.exit(summary.converged ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(2);
});
