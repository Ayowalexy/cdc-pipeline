import { Client } from '@elastic/elasticsearch';
import { LoggerService } from '@nestjs/common';
import { OrderChangeEvent, OrderStatus } from '../../libs/contracts/order-change-event';

export const ES_URL = `http://localhost:${process.env.ES_HOST_PORT ?? 9200}`;
export const KAFKA_BROKERS = [`localhost:${process.env.KAFKA_HOST_PORT ?? 29092}`];

export const esClient = () => new Client({ node: ES_URL });

export const uniqueSuffix = () => `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

const snapshot = (id: string, version: number, status: OrderStatus) => ({
  id,
  status,
  totalAmount: '149.90',
  currency: 'EUR',
  version,
  createdAt: '2026-11-02T10:00:00.000Z',
  updatedAt: new Date(Date.UTC(2026, 10, 2, 10, 0, version)).toISOString(),
});

export function upsertEvent(id: string, version: number, status: OrderStatus): OrderChangeEvent {
  const operation = version === 1 ? 'CREATE' : 'UPDATE';
  return {
    eventId: `order:${id}:v${version}:${operation}`,
    aggregateType: 'order',
    aggregateId: id,
    operation,
    version,
    occurredAt: new Date(Date.UTC(2026, 10, 2, 10, 0, version)).toISOString(),
    after: snapshot(id, version, status),
    before: version > 1 ? snapshot(id, version - 1, 'PENDING') : null,
  };
}

export function deleteEvent(id: string, lastVersion: number, lastStatus: OrderStatus): OrderChangeEvent {
  return {
    eventId: `order:${id}:v${lastVersion + 1}:DELETE`,
    aggregateType: 'order',
    aggregateId: id,
    operation: 'DELETE',
    version: lastVersion + 1,
    occurredAt: new Date(Date.UTC(2026, 10, 2, 11, 0, 0)).toISOString(),
    after: null,
    before: snapshot(id, lastVersion, lastStatus),
  };
}

export async function getDoc(client: Client, index: string, id: string) {
  const res = await client.get<Record<string, unknown>>({ index, id }, { ignore: [404] });
  return res.found ? { source: res._source!, seqNo: res._seq_no!, version: res._version! } : null;
}

/** Bounded polling for eventually consistent assertions. */
export async function eventually<T>(
  probe: () => Promise<T | undefined | null | false>,
  timeoutMs = 60_000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const v = await probe();
    if (v) return v;
    if (Date.now() > deadline) throw new Error('condition not reached within timeout');
    await new Promise((r) => setTimeout(r, 200));
  }
}

export type LogEntry = { level: string; message: Record<string, unknown> };

export class CapturingLogger implements LoggerService {
  readonly entries: LogEntry[] = [];
  log(message: Record<string, unknown>) {
    this.entries.push({ level: 'log', message });
  }
  warn(message: Record<string, unknown>) {
    this.entries.push({ level: 'warn', message });
  }
  error(message: Record<string, unknown>) {
    this.entries.push({ level: 'error', message });
  }
  outcomes(orderId?: string) {
    return this.entries
      .filter((e) => e.message.msg === 'order event' && (!orderId || e.message.orderId === orderId))
      .map((e) => ({
        version: e.message.incomingVersion,
        outcome: e.message.outcome,
        offset: e.message.correlationId,
      }));
  }
}
