/**
 * TC-04 consumer restart / replay against the real Kafka broker (:29092) and Elasticsearch (:9200).
 * Uses a private topic, DLQ topic, consumer group and index per run, so it never touches the
 * running cdc-consumer. Models kafkajs/Kafka offset behaviour exactly: what is re-read after a
 * restart is decided by the group's committed offsets on the broker.
 */
import { errors } from '@elastic/elasticsearch';
import { Admin, Kafka, logLevel } from 'kafkajs';
import { OrderEventsConsumer } from '../../apps/cdc-consumer/src/order-events.consumer';
import { OrderProjectionService } from '../../apps/cdc-consumer/src/order-projection.service';
import { OrderChangeEvent } from '../../libs/contracts/order-change-event';
import { created, deleted, key, updated } from '../fixtures/debezium';
import { CapturingLogger, eventually, esClient, getDoc, KAFKA_BROKERS, uniqueSuffix } from './helpers';

jest.setTimeout(240_000);

describe('OrderEventsConsumer recovery with a real Kafka broker', () => {
  const suffix = uniqueSuffix();
  const topic = `it-orders-${suffix}`;
  const dlqTopic = `${topic}.dlq`;
  const groupId = `it-projection-${suffix}`;
  const index = `orders-it-kafka-${suffix}`;
  const id = `ORD-IT-K-${suffix}`;

  const kafka = new Kafka({ clientId: 'it', brokers: KAFKA_BROKERS, logLevel: logLevel.NOTHING });
  const es = esClient();
  const projection = new OrderProjectionService(es, index);
  let admin: Admin;
  const producer = kafka.producer();

  const config = {
    brokers: KAFKA_BROKERS,
    clientId: 'it-consumer',
    groupId,
    topic,
    dlqTopic,
    maxAttempts: 3,
    retryBaseMs: 50,
    retryMaxMs: 200,
    restartOnFailure: false,
  };

  const send = (value: Buffer | null) => producer.send({ topic, messages: [{ key: key(id), value }] });
  const committed = async () => {
    const [t] = await admin.fetchOffsets({ groupId, topics: [topic] });
    return Number(t.partitions[0].offset);
  };
  const endOffset = async () => Number((await admin.fetchTopicOffsets(topic))[0].high);

  /** Records which offsets a consumer instance handled, then delegates to the real projection. */
  const spying = (
    seen: { version: number; offset?: string }[],
    inner: Pick<OrderProjectionService, 'apply'> = projection,
  ) => ({
    apply: (e: OrderChangeEvent) => {
      seen.push({ version: e.version, offset: e.sourcePosition?.offset });
      return inner.apply(e);
    },
  });

  beforeAll(async () => {
    admin = kafka.admin();
    await admin.connect();
    await admin.createTopics({
      topics: [
        { topic, numPartitions: 1 },
        { topic: dlqTopic, numPartitions: 1 },
      ],
      waitForLeaders: true,
    });
    await producer.connect();
    await projection.ensureIndex();
  });

  afterAll(async () => {
    await producer.disconnect();
    await admin.deleteGroups([groupId]).catch(() => undefined);
    await admin.deleteTopics({ topics: [topic, dlqTopic] }).catch(() => undefined);
    await admin.disconnect();
    await es.indices.delete({ index }, { ignore: [404] });
    await es.close();
  });

  it('resumes from the committed offset after a clean stop, without re-reading committed events', async () => {
    await send(created(id)); // offset 0, v1
    await send(updated(id, 2, 'CONFIRMED')); // offset 1, v2

    const a = new OrderEventsConsumer(config, projection, new CapturingLogger());
    await a.start();
    await eventually(async () => (await committed()) === 2);
    await a.stop();
    expect((await getDoc(es, index, id))?.source).toMatchObject({ version: 2, status: 'CONFIRMED' });

    // Events published while the consumer is down stay pending in the broker (lag 2).
    await send(updated(id, 3, 'PROCESSING', 'CONFIRMED')); // offset 2
    await send(updated(id, 4, 'COMPLETED', 'PROCESSING')); // offset 3
    expect(await committed()).toBe(2);
    expect(await endOffset()).toBe(4);

    const seen: { version: number; offset?: string }[] = [];
    const b = new OrderEventsConsumer(config, spying(seen), new CapturingLogger());
    await b.start();
    await eventually(async () => (await committed()) === 4);
    await b.stop();

    expect(seen).toEqual([
      { version: 3, offset: '2' },
      { version: 4, offset: '3' },
    ]);
    expect((await getDoc(es, index, id))?.source).toMatchObject({ version: 4, status: 'COMPLETED' });
  });

  it('does not commit an event whose processing failed; it is redelivered after restart', async () => {
    await send(updated(id, 5, 'CANCELLED', 'COMPLETED')); // offset 4

    let failures = 0;
    const failing = {
      apply: async () => {
        failures++;
        throw new errors.ConnectionError('simulated: Elasticsearch unreachable');
      },
    };
    const logger = new CapturingLogger();
    const c = new OrderEventsConsumer(config, failing, logger);
    await c.start();
    await eventually(async () => failures >= config.maxAttempts);
    await eventually(async () => !c.isRunning); // retries exhausted -> consumer crashed without commit
    await c.stop();
    expect(await committed()).toBe(4);
    expect(logger.outcomes(id).map((o) => o.outcome)).toEqual([
      'retryable_error',
      'retryable_error',
      'retryable_error',
    ]);
    expect((await getDoc(es, index, id))?.source).toMatchObject({ version: 4 });

    const seen: { version: number; offset?: string }[] = [];
    const d = new OrderEventsConsumer(config, spying(seen), new CapturingLogger());
    await d.start();
    await eventually(async () => (await committed()) === 5);
    await d.stop();
    expect(seen).toEqual([{ version: 5, offset: '4' }]);
    expect((await getDoc(es, index, id))?.source).toMatchObject({ version: 5, status: 'CANCELLED' });
  });

  it('is idempotent when the write succeeded but the outcome was not acknowledged (crash between write and commit)', async () => {
    await send(deleted(id, 5, 'CANCELLED')); // offset 5, delete -> tombstone v6
    await send(null); // offset 6, Kafka tombstone

    let calls = 0;
    const writeThenFail = {
      apply: async (e: OrderChangeEvent) => {
        const result = await projection.apply(e);
        if (calls++ === 0) throw new errors.ConnectionError('simulated: connection lost after the write');
        return result;
      },
    };
    const logger = new CapturingLogger();
    const e = new OrderEventsConsumer(config, writeThenFail, logger);
    await e.start();
    await eventually(async () => (await committed()) === 7);
    await e.stop();

    expect(logger.outcomes(id).map((o) => o.outcome)).toEqual(['retryable_error', 'duplicate']);
    expect((await getDoc(es, index, id))?.source).toMatchObject({ version: 6, is_deleted: true });
  });

  it('real broker replay: rewinding the group to earliest re-reads every event without regressing state', async () => {
    const before = await getDoc(es, index, id);
    await admin.resetOffsets({ groupId, topic, earliest: true });
    expect(await committed()).toBeLessThan(1); // rewound (earliest = 0, or the -2 "earliest" sentinel)
    const seen: { version: number; offset?: string }[] = [];
    const logger = new CapturingLogger();
    const f = new OrderEventsConsumer(config, spying(seen), logger);
    await f.start();
    await eventually(async () => (await committed()) === 7);
    await f.stop();

    expect(seen.map((s) => s.offset)).toEqual(['0', '1', '2', '3', '4', '5']); // offset 6 is the Kafka tombstone
    expect(logger.outcomes(id).map((o) => o.outcome)).toEqual([
      'ignored_stale',
      'ignored_stale',
      'ignored_stale',
      'ignored_stale',
      'ignored_stale',
      'duplicate',
    ]);
    const after = await getDoc(es, index, id);
    expect(after?.source).toMatchObject({ version: 6, is_deleted: true });
    expect(after?.seqNo).toBe(before?.seqNo); // nothing was rewritten during the replay
  });

  it('routes an unparseable record to the DLQ and moves on (permanent error)', async () => {
    await producer.send({ topic, messages: [{ key: key(id), value: Buffer.from('{"op":"u","after":') }] }); // offset 7
    const logger = new CapturingLogger();
    const g = new OrderEventsConsumer(config, projection, logger);
    await g.start();
    await eventually(async () => (await committed()) === 8);
    await g.stop();

    expect(logger.entries.some((e) => e.message.outcome === 'permanent_error')).toBe(true);
    expect(Number((await admin.fetchTopicOffsets(dlqTopic))[0].high)).toBe(1);
  });
});
