/**
 * Debezium 2.7 / pgoutput / JsonConverter(schemas.enable=false) envelopes, matching
 * infrastructure/debezium/orders-connector.json. Shapes were checked against records captured
 * from the running connector (see test/fixtures/captured-update.json).
 */
type Row = {
  id: string;
  status: string;
  total_amount: string;
  currency: string | null;
  version: number;
  created_at: string;
  updated_at: string;
};

export function row(id: string, version: number, status: string): Row {
  return {
    id,
    status,
    total_amount: '149.90',
    currency: 'EUR',
    version,
    created_at: '2026-11-02T10:00:00.000000Z',
    updated_at: `2026-11-02T10:00:${String(version).padStart(2, '0')}.000000Z`,
  };
}

let lsn = 30_000_000;

export function envelope(op: 'c' | 'u' | 'd' | 'r', before: Row | null, after: Row | null) {
  lsn += 100;
  return {
    before,
    after,
    source: {
      version: '2.7.3.Final',
      connector: 'postgresql',
      name: 'cdc',
      ts_ms: 1793613600000 + lsn,
      snapshot: op === 'r' ? 'true' : 'false',
      db: 'orders',
      sequence: `["${lsn - 100}","${lsn}"]`,
      schema: 'public',
      table: 'orders',
      txId: 700 + (lsn % 1000),
      lsn,
      xmin: null,
    },
    op,
    ts_ms: 1793613600000 + lsn + 5,
    transaction: null,
  };
}

export const key = (id: string) => Buffer.from(JSON.stringify({ id }));
export const value = (env: unknown) => Buffer.from(JSON.stringify(env));

/** Debezium record for an INSERT / UPDATE / DELETE of an order. */
export const created = (id: string) => value(envelope('c', null, row(id, 1, 'PENDING')));
export const updated = (id: string, version: number, status: string, prevStatus = 'PENDING') =>
  value(envelope('u', row(id, version - 1, prevStatus), row(id, version, status)));
export const deleted = (id: string, lastVersion: number, lastStatus: string) =>
  value(envelope('d', row(id, lastVersion, lastStatus), null));
