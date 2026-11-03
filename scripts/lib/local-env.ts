/**
 * Helpers shared by the operator scripts (seed, reproduce, replay, scenarios).
 * They talk to the stack through its host ports, exactly as a developer would.
 */
import { execFileSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { Kafka, KafkaMessage, logLevel } from 'kafkajs';
import { Client as PgClient } from 'pg';
import { normalizeDebeziumOrderMessage } from '../../libs/event-normalizer/debezium-order.adapter';
import { OrderChangeEvent, OrderDocument, OrderSnapshot } from '../../libs/contracts/order-change-event';

export const env = {
  orderServiceUrl: `http://localhost:${process.env.ORDER_SERVICE_HOST_PORT ?? 3100}`,
  esUrl: `http://localhost:${process.env.ES_HOST_PORT ?? 9200}`,
  esIndex: process.env.ES_ORDERS_INDEX ?? 'orders',
  kafkaBrokers: [`localhost:${process.env.KAFKA_HOST_PORT ?? 29092}`],
  topic: process.env.KAFKA_ORDERS_TOPIC ?? 'cdc.public.orders',
  groupId: process.env.KAFKA_GROUP_ID ?? 'order-projection',
  pg: {
    host: 'localhost',
    port: Number(process.env.POSTGRES_HOST_PORT ?? 55432),
    user: process.env.POSTGRES_USER ?? 'orders',
    password: process.env.POSTGRES_PASSWORD ?? 'orders_local_only',
    database: process.env.POSTGRES_DB ?? 'orders',
  },
};

export const kafka = new Kafka({ clientId: 'cdc-scripts', brokers: env.kafkaBrokers, logLevel: logLevel.NOTHING });

export async function api<T = OrderSnapshot>(method: string, path: string, body?: unknown): Promise<T> {
  const res = await fetch(`${env.orderServiceUrl}${path}`, {
    method,
    headers: body === undefined ? undefined : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`${method} ${path} -> HTTP ${res.status}: ${text}`);
  return JSON.parse(text) as T;
}

/** Source of truth, read straight from PostgreSQL (not through the API). */
export async function pgOrder(id: string): Promise<{ status: string; version: number } | null> {
  const client = new PgClient(env.pg);
  await client.connect();
  try {
    const { rows } = await client.query('SELECT status, version FROM orders WHERE id = $1', [id]);
    return rows[0] ?? null;
  } finally {
    await client.end();
  }
}

export type EsDoc = { found: boolean; seqNo?: number; source?: OrderDocument };

/** Realtime GET of the projection document (no refresh needed). */
export async function esDoc(id: string): Promise<EsDoc> {
  const res = await fetch(`${env.esUrl}/${env.esIndex}/_doc/${encodeURIComponent(id)}`);
  if (res.status === 404) return { found: false };
  if (!res.ok) throw new Error(`GET ${env.esIndex}/_doc/${id} -> HTTP ${res.status}`);
  const body = (await res.json()) as { _seq_no: number; _source: OrderDocument };
  return { found: true, seqNo: body._seq_no, source: body._source };
}

/** The application search query: live (non-deleted) orders only. */
export async function esSearchLive(id: string): Promise<number> {
  await fetch(`${env.esUrl}/${env.esIndex}/_refresh`, { method: 'POST' });
  const res = await fetch(`${env.esUrl}/${env.esIndex}/_search`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      query: { bool: { filter: [{ term: { order_id: id } }, { term: { is_deleted: false } }] } },
    }),
  });
  const body = (await res.json()) as { hits: { total: { value: number } } };
  return body.hits.total.value;
}

/** Bounded polling: resolves with the first truthy value or throws after timeoutMs. */
export async function waitFor<T>(
  description: string,
  probe: () => Promise<T | undefined | null | false>,
  { timeoutMs = 60_000, intervalMs = 250 } = {},
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await probe();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`timed out after ${timeoutMs}ms waiting for: ${description}`);
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}

export function waitForEsVersion(id: string, version: number, timeoutMs = 60_000) {
  return waitFor(
    `${id} indexed at version ${version}`,
    async () => {
      const doc = await esDoc(id);
      return doc.source?.version === version ? doc : undefined;
    },
    { timeoutMs },
  );
}

export type CapturedMessage = {
  partition: number;
  offset: string;
  key: Buffer | null;
  value: Buffer | null;
  event: OrderChangeEvent;
};

/**
 * Finds the real Debezium record(s) for an order by scanning the CDC topic with a throwaway
 * consumer group (deleted afterwards). Does not touch the projection consumer group.
 */
export async function findOrderMessages(orderId: string): Promise<CapturedMessage[]> {
  const admin = kafka.admin();
  await admin.connect();
  const ends = await admin.fetchTopicOffsets(env.topic);
  const pending = new Map(ends.filter((p) => p.high !== p.low).map((p) => [p.partition, BigInt(p.high) - 1n]));
  const groupId = `scan-${randomUUID()}`;
  const found: CapturedMessage[] = [];

  if (pending.size > 0) {
    const consumer = kafka.consumer({ groupId });
    await consumer.connect();
    await consumer.subscribe({ topic: env.topic, fromBeginning: true });
    await new Promise<void>((resolve, reject) => {
      consumer
        .run({
          autoCommit: false,
          eachMessage: async ({ partition, message }) => {
            collect(orderId, partition, message, found);
            if (pending.has(partition) && BigInt(message.offset) >= pending.get(partition)!) pending.delete(partition);
            if (pending.size === 0) resolve();
          },
        })
        .catch(reject);
    });
    await consumer.disconnect();
    await admin.deleteGroups([groupId]).catch(() => undefined);
  }
  await admin.disconnect();
  return found;
}

function collect(orderId: string, partition: number, message: KafkaMessage, into: CapturedMessage[]) {
  if (message.headers?.['x-replay']) return; // only original Debezium records
  try {
    const result = normalizeDebeziumOrderMessage(message.key, message.value, { partition, offset: message.offset });
    if (result.kind === 'event' && result.event.aggregateId === orderId) {
      into.push({ partition, offset: message.offset, key: message.key, value: message.value, event: result.event });
    }
  } catch {
    // not an order event we can parse; irrelevant for the scan
  }
}

/**
 * Controlled replay fixture: re-publishes a captured Debezium record (same key, same bytes) to
 * the same topic, which is what a producer retry or an upstream re-send looks like to the consumer.
 * The copy carries `x-replay` headers so logs can tell it apart from the original.
 */
export async function replayMessage(msg: CapturedMessage): Promise<{ partition: number; offset: string }> {
  const producer = kafka.producer();
  await producer.connect();
  try {
    const [meta] = await producer.send({
      topic: env.topic,
      messages: [
        {
          key: msg.key,
          value: msg.value,
          partition: msg.partition,
          headers: { 'x-replay': 'controlled', 'x-replay-of': `${msg.partition}/${msg.offset}` },
        },
      ],
    });
    return { partition: meta.partition, offset: meta.baseOffset! };
  } finally {
    await producer.disconnect();
  }
}

/** Explicit synchronisation: the projection group has committed past `offset` on `partition`. */
export async function waitForCommitted(partition: number, offset: string, timeoutMs = 60_000) {
  const admin = kafka.admin();
  await admin.connect();
  try {
    return await waitFor(
      `consumer group ${env.groupId} committed past ${env.topic}/${partition}/${offset}`,
      async () => {
        const committed = await committedOffset(admin, partition);
        return committed !== null && committed > BigInt(offset) ? committed : undefined;
      },
      { timeoutMs },
    );
  } finally {
    await admin.disconnect();
  }
}

async function committedOffset(admin: ReturnType<Kafka['admin']>, partition: number): Promise<bigint | null> {
  const [topicOffsets] = await admin.fetchOffsets({ groupId: env.groupId, topics: [env.topic] });
  const entry = topicOffsets?.partitions.find((p) => p.partition === partition);
  return entry && entry.offset !== '-1' ? BigInt(entry.offset) : null;
}

/** Committed offsets and log-end offsets per partition (lag = end - committed). */
export async function groupLag() {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const ends = await admin.fetchTopicOffsets(env.topic);
    const [committed] = await admin.fetchOffsets({ groupId: env.groupId, topics: [env.topic] });
    return ends
      .map((e) => {
        const c = committed?.partitions.find((p) => p.partition === e.partition)?.offset ?? '-1';
        // -1 = no committed offset, -2 = "earliest" sentinel left by an offset reset: both mean "from the start".
        const lag = BigInt(c) < 0n ? BigInt(e.high) - BigInt(e.low) : BigInt(e.high) - BigInt(c);
        return { partition: e.partition, committed: c, end: e.high, lag: Number(lag) };
      })
      .sort((a, b) => a.partition - b.partition);
  } finally {
    await admin.disconnect();
  }
}

export async function waitForZeroLag(timeoutMs = 120_000) {
  return waitFor(
    'projection consumer lag = 0 on all partitions',
    async () => {
      const lag = await groupLag();
      return lag.every((p) => p.lag === 0) ? lag : undefined;
    },
    { timeoutMs, intervalMs: 500 },
  );
}

/** Kafka only allows offset resets on a group with no members; wait (bounded) until it is Empty. */
export async function waitForGroupEmpty(timeoutMs = 60_000) {
  const admin = kafka.admin();
  await admin.connect();
  try {
    return await waitFor(
      `consumer group ${env.groupId} to be Empty`,
      async () => {
        const { groups } = await admin.describeGroups([env.groupId]);
        return groups[0]?.state === 'Empty' ? groups[0].state : undefined;
      },
      { timeoutMs, intervalMs: 500 },
    );
  } finally {
    await admin.disconnect();
  }
}

/** Real broker replay: rewinds the projection group to the earliest offset (group must be inactive). */
export async function resetGroupToEarliest() {
  await waitForGroupEmpty();
  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.resetOffsets({ groupId: env.groupId, topic: env.topic, earliest: true });
  } finally {
    await admin.disconnect();
  }
}

export function compose(...args: string[]): string {
  return execFileSync('docker', ['compose', ...args], {
    cwd: `${__dirname}/../..`,
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
  });
}

export function consumerLogsSince(sinceIso: string): string {
  return compose('logs', '--no-color', '--no-log-prefix', '--since', sinceIso, 'cdc-consumer');
}

export function runId(): string {
  return new Date().toISOString().replace(/[-:]/g, '').replace(/\..*/, '').replace('T', '-');
}
