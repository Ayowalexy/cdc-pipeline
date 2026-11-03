/**
 * Seeds a few synthetic orders (idempotent: existing IDs are left untouched) and waits until the
 * projection has caught up.   npm run seed
 */
import { api, esDoc, pgOrder, waitForEsVersion } from './lib/local-env';

const SEED: { id: string; totalAmount: string; currency: string; statuses: string[] }[] = [
  { id: 'ORD-1001', totalAmount: '149.90', currency: 'EUR', statuses: ['CONFIRMED'] },
  { id: 'ORD-1002', totalAmount: '20.00', currency: 'USD', statuses: [] },
  { id: 'ORD-1003', totalAmount: '310.45', currency: 'GBP', statuses: ['CONFIRMED', 'PROCESSING'] },
  { id: 'ORD-1004', totalAmount: '75.10', currency: 'EUR', statuses: ['CONFIRMED', 'PROCESSING', 'COMPLETED'] },
  { id: 'ORD-1005', totalAmount: '12.99', currency: 'USD', statuses: ['CANCELLED'] },
];

async function main() {
  for (const seed of SEED) {
    if (await pgOrder(seed.id)) {
      console.log(`${seed.id}: already present, skipped`);
      continue;
    }
    await api('POST', '/orders', { id: seed.id, totalAmount: seed.totalAmount, currency: seed.currency });
    for (const status of seed.statuses) await api('PATCH', `/orders/${seed.id}/status`, { status });
    console.log(`${seed.id}: created`);
  }
  for (const seed of SEED) {
    const pg = await pgOrder(seed.id);
    if (!pg) continue;
    await waitForEsVersion(seed.id, pg.version);
    const es = (await esDoc(seed.id)).source!;
    console.log(`${seed.id}: PostgreSQL ${pg.status}/v${pg.version}  Elasticsearch ${es.status}/v${es.version}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
