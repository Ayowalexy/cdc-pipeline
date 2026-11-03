import {
  EventNormalizationError,
  normalizeDebeziumOrderMessage,
} from '../../libs/event-normalizer/debezium-order.adapter';
import { created, deleted, envelope, key, row, updated, value } from '../fixtures/debezium';

const pos = { partition: 1, offset: '42' };

function event(k: Buffer | null, v: Buffer | null) {
  const result = normalizeDebeziumOrderMessage(k, v, pos);
  if (result.kind !== 'event') throw new Error(`expected event, got ${result.kind}`);
  return result.event;
}

describe('normalizeDebeziumOrderMessage', () => {
  it('maps an insert (op=c) to CREATE at version 1', () => {
    const e = event(key('ORD-1'), created('ORD-1'));
    expect(e).toMatchObject({
      eventId: 'order:ORD-1:v1:CREATE',
      aggregateType: 'order',
      aggregateId: 'ORD-1',
      operation: 'CREATE',
      version: 1,
      before: null,
      after: { id: 'ORD-1', status: 'PENDING', totalAmount: '149.90', currency: 'EUR', version: 1 },
    });
    expect(e.sourcePosition).toMatchObject({ partition: 1, offset: '42' });
    expect(e.sourcePosition?.lsn).toMatch(/^\d+$/);
    expect(e.sourcePosition?.transactionId).toMatch(/^\d+$/);
    expect(new Date(e.occurredAt).toString()).not.toBe('Invalid Date');
  });

  it('maps a snapshot read (op=r) to CREATE', () => {
    const e = event(key('ORD-1'), value(envelope('r', null, row('ORD-1', 3, 'PROCESSING'))));
    expect(e).toMatchObject({ operation: 'CREATE', version: 3 });
  });

  it('maps an update (op=u) to UPDATE using the row version from `after`', () => {
    const e = event(key('ORD-1'), updated('ORD-1', 4, 'COMPLETED', 'PROCESSING'));
    expect(e).toMatchObject({ operation: 'UPDATE', version: 4, eventId: 'order:ORD-1:v4:UPDATE' });
    expect(e.after?.status).toBe('COMPLETED');
    expect(e.before?.status).toBe('PROCESSING');
  });

  it('maps a delete (op=d) to DELETE ordered after the last row version', () => {
    const e = event(key('ORD-1'), deleted('ORD-1', 4, 'COMPLETED'));
    expect(e).toMatchObject({ operation: 'DELETE', version: 5, after: null, aggregateId: 'ORD-1' });
    expect(e.before?.status).toBe('COMPLETED');
  });

  it('yields the same eventId for a redelivered copy of the same record', () => {
    const bytes = updated('ORD-1', 2, 'CONFIRMED');
    const a = normalizeDebeziumOrderMessage(key('ORD-1'), bytes, { partition: 0, offset: '1' });
    const b = normalizeDebeziumOrderMessage(key('ORD-1'), bytes, { partition: 0, offset: '9' });
    expect(a.kind === 'event' && b.kind === 'event' && a.event.eventId === b.event.eventId).toBe(true);
  });

  it('recognises the Kafka tombstone (null value) that follows a delete', () => {
    expect(normalizeDebeziumOrderMessage(key('ORD-1'), null, pos)).toEqual({
      kind: 'kafka_tombstone',
      aggregateId: 'ORD-1',
    });
  });

  it.each([
    ['invalid JSON', Buffer.from('{not json')],
    ['unsupported op (truncate)', value({ ...envelope('u', null, row('ORD-1', 1, 'PENDING')), op: 't' })],
    ['delete without before image (REPLICA IDENTITY DEFAULT)', value(envelope('d', null, null))],
    ['update without after image', value(envelope('u', row('ORD-1', 1, 'PENDING'), null))],
    ['unknown status', value(envelope('u', null, row('ORD-1', 2, 'SHIPPED')))],
    ['non-integer version', value(envelope('u', null, { ...row('ORD-1', 2, 'CONFIRMED'), version: 2.5 }))],
  ])('rejects %s as a permanent normalization error', (_label, bytes) => {
    expect(() => normalizeDebeziumOrderMessage(key('ORD-1'), bytes, pos)).toThrow(EventNormalizationError);
  });
});
