export const ORDER_STATUSES = ['PENDING', 'CONFIRMED', 'PROCESSING', 'COMPLETED', 'CANCELLED'] as const;
export type OrderStatus = (typeof ORDER_STATUSES)[number];

/** Row shape as owned by PostgreSQL (the source of truth). */
export type OrderSnapshot = {
  id: string;
  status: OrderStatus;
  totalAmount: string;
  currency: string | null;
  version: number;
  createdAt: string;
  updatedAt: string;
};

/**
 * Connector-agnostic change event. Projection logic only ever sees this type;
 * Debezium envelope parsing lives in libs/event-normalizer.
 */
export type OrderChangeEvent = {
  eventId: string;
  aggregateType: 'order';
  aggregateId: string;
  operation: 'CREATE' | 'UPDATE' | 'DELETE';
  version: number;
  occurredAt: string;
  sourcePosition?: {
    transactionId?: string;
    lsn?: string;
    partition?: number;
    offset?: string;
  };
  /** Row after the change; null for DELETE. */
  after: OrderSnapshot | null;
  /** Row before the change when the connector provides it (REPLICA IDENTITY FULL). */
  before: OrderSnapshot | null;
};

/** Document stored in the Elasticsearch `orders` index (snake_case, see infrastructure/elasticsearch). */
export type OrderDocument = {
  order_id: string;
  status: OrderStatus | null;
  total_amount: number | null;
  currency: string | null;
  version: number;
  is_deleted: boolean;
  created_at: string | null;
  updated_at: string | null;
  deleted_at: string | null;
  last_event_id: string;
};

export type ProjectionOutcome =
  | 'applied'
  | 'ignored_stale'
  | 'duplicate'
  | 'deleted'
  | 'retryable_error'
  | 'permanent_error';
