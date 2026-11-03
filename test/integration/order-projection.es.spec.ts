/**
 * Real Elasticsearch (docker compose service on :9200). Each test run uses its own index,
 * each test its own order ID; the index is deleted afterwards.
 */
import { Client } from '@elastic/elasticsearch';
import { OrderProjectionService } from '../../apps/cdc-consumer/src/order-projection.service';
import { deleteEvent, esClient, getDoc, uniqueSuffix, upsertEvent } from './helpers';

describe('OrderProjectionService against real Elasticsearch', () => {
  let client: Client;
  let projection: OrderProjectionService;
  const index = `orders-it-${uniqueSuffix()}`;
  let id: string;

  beforeAll(async () => {
    client = esClient();
    projection = new OrderProjectionService(client, index);
    await projection.ensureIndex();
    await projection.ensureIndex(); // idempotent
  });

  afterAll(async () => {
    await client.indices.delete({ index }, { ignore: [404] });
    await client.close();
  });

  beforeEach(() => {
    id = `ORD-IT-${uniqueSuffix()}`;
  });

  it('TC-01 normal update: a newer version replaces the document', async () => {
    expect(await projection.apply(upsertEvent(id, 1, 'PENDING'))).toEqual({ outcome: 'applied' });
    expect(await projection.apply(upsertEvent(id, 2, 'CONFIRMED'))).toEqual({ outcome: 'applied' });
    expect((await getDoc(client, index, id))?.source).toMatchObject({
      order_id: id,
      status: 'CONFIRMED',
      version: 2,
      is_deleted: false,
    });
  });

  it('TC-02 rapid updates: final document matches the highest version', async () => {
    const statuses = ['PENDING', 'CONFIRMED', 'PROCESSING', 'CONFIRMED', 'PROCESSING', 'COMPLETED'] as const;
    for (const [i, status] of statuses.entries()) await projection.apply(upsertEvent(id, i + 1, status));
    expect((await getDoc(client, index, id))?.source).toMatchObject({ status: 'COMPLETED', version: 6 });
  });

  it('TC-02 concurrent writes of interleaved versions converge to the highest version (atomic compare-and-write)', async () => {
    const statuses = ['PENDING', 'CONFIRMED', 'PROCESSING', 'COMPLETED', 'CANCELLED'] as const;
    const events = statuses.map((s, i) => upsertEvent(id, i + 1, s));
    // Fire all writes at once, newest first, so lower versions race against an already-higher doc.
    await Promise.all([...events].reverse().map((e) => projection.apply(e)));
    expect((await getDoc(client, index, id))?.source).toMatchObject({ status: 'CANCELLED', version: 5 });
  });

  it('TC-03 duplicate event: re-applying the same version is a no-op reported as duplicate', async () => {
    await projection.apply(upsertEvent(id, 1, 'PENDING'));
    await projection.apply(upsertEvent(id, 2, 'CONFIRMED'));
    const before = await getDoc(client, index, id);

    expect(await projection.apply(upsertEvent(id, 2, 'CONFIRMED'))).toEqual({
      outcome: 'duplicate',
      indexedVersion: 2,
    });
    const after = await getDoc(client, index, id);
    expect(after?.source).toMatchObject({ status: 'CONFIRMED', version: 2 });
    expect(after?.seqNo).toBe(before?.seqNo); // document not rewritten
  });

  it('TC-05 out-of-order event: a lower version never overwrites a higher one', async () => {
    await projection.apply(upsertEvent(id, 3, 'PROCESSING'));
    expect(await projection.apply(upsertEvent(id, 2, 'CONFIRMED'))).toEqual({
      outcome: 'ignored_stale',
      indexedVersion: 3,
    });
    expect(await projection.apply(upsertEvent(id, 1, 'PENDING'))).toEqual({
      outcome: 'ignored_stale',
      indexedVersion: 3,
    });
    expect((await getDoc(client, index, id))?.source).toMatchObject({ status: 'PROCESSING', version: 3 });
  });

  it('regression (seeded defect): v2 delivered after v4 leaves the document at v4 / COMPLETED', async () => {
    await projection.apply(upsertEvent(id, 1, 'PENDING'));
    await projection.apply(upsertEvent(id, 2, 'CONFIRMED'));
    await projection.apply(upsertEvent(id, 3, 'PROCESSING'));
    await projection.apply(upsertEvent(id, 4, 'COMPLETED'));
    await projection.apply(upsertEvent(id, 2, 'CONFIRMED')); // late / replayed
    expect((await getDoc(client, index, id))?.source).toMatchObject({ status: 'COMPLETED', version: 4 });
  });

  it('TC-06 deletion: writes a versioned tombstone excluded from search', async () => {
    await projection.apply(upsertEvent(id, 1, 'PENDING'));
    await projection.apply(upsertEvent(id, 2, 'CONFIRMED'));
    expect(await projection.apply(deleteEvent(id, 2, 'CONFIRMED'))).toEqual({ outcome: 'deleted' });

    expect((await getDoc(client, index, id))?.source).toMatchObject({ is_deleted: true, version: 3 });
    await client.indices.refresh({ index });
    const live = await client.search({
      index,
      query: { bool: { filter: [{ term: { order_id: id } }, { term: { is_deleted: false } }] } },
    });
    expect(live.hits.hits).toHaveLength(0);
  });

  it('TC-06 stale resurrection: an older update after the delete cannot recreate the order', async () => {
    await projection.apply(upsertEvent(id, 1, 'PENDING'));
    await projection.apply(upsertEvent(id, 2, 'CONFIRMED'));
    await projection.apply(deleteEvent(id, 2, 'CONFIRMED'));

    expect(await projection.apply(upsertEvent(id, 2, 'CONFIRMED'))).toEqual({
      outcome: 'ignored_stale',
      indexedVersion: 3,
    });
    expect(await projection.apply(upsertEvent(id, 1, 'PENDING'))).toEqual({
      outcome: 'ignored_stale',
      indexedVersion: 3,
    });
    expect((await getDoc(client, index, id))?.source).toMatchObject({ is_deleted: true, version: 3 });
  });

  it('TC-06 a duplicate delete is reported as duplicate and keeps the tombstone', async () => {
    await projection.apply(upsertEvent(id, 1, 'PENDING'));
    await projection.apply(deleteEvent(id, 1, 'PENDING'));
    expect(await projection.apply(deleteEvent(id, 1, 'PENDING'))).toEqual({ outcome: 'duplicate', indexedVersion: 2 });
    expect((await getDoc(client, index, id))?.source).toMatchObject({ is_deleted: true, version: 2 });
  });

  it('a delete that arrives before earlier updates still wins', async () => {
    await projection.apply(deleteEvent(id, 3, 'PROCESSING'));
    expect((await projection.apply(upsertEvent(id, 3, 'PROCESSING'))).outcome).toBe('ignored_stale');
    expect((await getDoc(client, index, id))?.source).toMatchObject({ is_deleted: true, version: 4 });
  });
});
