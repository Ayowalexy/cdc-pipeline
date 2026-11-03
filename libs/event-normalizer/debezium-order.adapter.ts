import { ORDER_STATUSES, OrderChangeEvent, OrderSnapshot, OrderStatus } from '../contracts/order-change-event';

/**
 * Adapter for the pinned Debezium configuration in infrastructure/debezium/orders-connector.json:
 *   Debezium 2.7 PostgreSQL connector, pgoutput, JsonConverter with schemas.enable=false,
 *   decimal.handling.mode=string, time.precision.mode=adaptive (timestamptz -> ISO-8601 string),
 *   REPLICA IDENTITY FULL on public.orders (delete events carry the full `before` row).
 *
 * Envelope (value): { before, after, source: { lsn, txId, ts_ms, snapshot, table, ... }, op, ts_ms }
 * op: c = insert, u = update, d = delete, r = snapshot read. A null value is the Kafka tombstone
 * Debezium emits after a delete for log compaction.
 */

export class EventNormalizationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'EventNormalizationError';
  }
}

export type KafkaPosition = { partition: number; offset: string };

export type NormalizeResult =
  | { kind: 'event'; event: OrderChangeEvent }
  | { kind: 'kafka_tombstone'; aggregateId: string | null };

type DebeziumRow = {
  id: string;
  status: string;
  total_amount: string | null;
  currency: string | null;
  version: number;
  created_at: string;
  updated_at: string;
};

type DebeziumEnvelope = {
  before: DebeziumRow | null;
  after: DebeziumRow | null;
  source?: { lsn?: number | string; txId?: number | string; ts_ms?: number; table?: string };
  op: 'c' | 'u' | 'd' | 'r' | 't' | 'm';
  ts_ms?: number;
};

const OPERATIONS: Record<string, OrderChangeEvent['operation'] | undefined> = {
  c: 'CREATE',
  r: 'CREATE',
  u: 'UPDATE',
  d: 'DELETE',
};

export function normalizeDebeziumOrderMessage(
  key: Buffer | null,
  value: Buffer | null,
  position: KafkaPosition,
): NormalizeResult {
  if (value === null || value.length === 0) {
    return { kind: 'kafka_tombstone', aggregateId: parseKeyId(key) };
  }

  let envelope: DebeziumEnvelope;
  try {
    envelope = JSON.parse(value.toString('utf8')) as DebeziumEnvelope;
  } catch {
    throw new EventNormalizationError('value is not valid JSON');
  }

  const operation = OPERATIONS[envelope?.op];
  if (!operation) {
    throw new EventNormalizationError(`unsupported Debezium op "${String(envelope?.op)}"`);
  }

  const before = envelope.before ? toSnapshot(envelope.before, 'before') : null;
  const after = envelope.after ? toSnapshot(envelope.after, 'after') : null;

  let version: number;
  let aggregateId: string;
  if (operation === 'DELETE') {
    if (!before) {
      throw new EventNormalizationError(
        'delete event has no before image; public.orders must use REPLICA IDENTITY FULL',
      );
    }
    aggregateId = before.id;
    // A delete is the terminal state change of an order, so it is ordered after the last row
    // version. Order IDs are never re-used, so before.version + 1 cannot collide with an update.
    version = before.version + 1;
  } else {
    if (!after) {
      throw new EventNormalizationError(`${operation} event has no after image`);
    }
    aggregateId = after.id;
    version = after.version;
  }

  const lsn = envelope.source?.lsn !== undefined ? String(envelope.source.lsn) : undefined;
  const txId = envelope.source?.txId !== undefined ? String(envelope.source.txId) : undefined;
  const occurredMs = envelope.source?.ts_ms ?? envelope.ts_ms;

  return {
    kind: 'event',
    event: {
      // Deterministic: a redelivered copy of the same change yields the same eventId.
      eventId: `order:${aggregateId}:v${version}:${operation}`,
      aggregateType: 'order',
      aggregateId,
      operation,
      version,
      occurredAt: occurredMs !== undefined ? new Date(occurredMs).toISOString() : new Date(0).toISOString(),
      sourcePosition: {
        transactionId: txId,
        lsn,
        partition: position.partition,
        offset: position.offset,
      },
      after,
      before,
    },
  };
}

function toSnapshot(row: DebeziumRow, label: string): OrderSnapshot {
  if (typeof row.id !== 'string' || row.id.length === 0) {
    throw new EventNormalizationError(`${label}.id missing`);
  }
  if (!Number.isInteger(row.version) || row.version < 1) {
    throw new EventNormalizationError(`${label}.version must be a positive integer, got ${String(row.version)}`);
  }
  if (!ORDER_STATUSES.includes(row.status as OrderStatus)) {
    throw new EventNormalizationError(`${label}.status "${String(row.status)}" is not a known order status`);
  }
  return {
    id: row.id,
    status: row.status as OrderStatus,
    totalAmount: row.total_amount ?? '0',
    currency: row.currency ?? null,
    version: row.version,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function parseKeyId(key: Buffer | null): string | null {
  if (!key) return null;
  try {
    const parsed = JSON.parse(key.toString('utf8')) as { id?: unknown };
    return typeof parsed.id === 'string' ? parsed.id : null;
  } catch {
    return null;
  }
}
