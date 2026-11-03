import { Client } from '@elastic/elasticsearch';
import ordersIndexDefinition from '../../../infrastructure/elasticsearch/orders-index.json';
import { OrderChangeEvent, OrderDocument, ProjectionOutcome } from '../../../libs/contracts/order-change-event';

export type ProjectionResult = { outcome: ProjectionOutcome; indexedVersion?: number };

/**
 * Maintains the Elasticsearch `orders` projection. Document ID = PostgreSQL order ID.
 * Deletes are written as tombstone documents (is_deleted: true) rather than physically removed.
 */
export class OrderProjectionService {
  constructor(
    private readonly client: Client,
    readonly index: string,
  ) {}

  /** Idempotent: creates the index with explicit mappings if it does not exist. */
  async ensureIndex(): Promise<void> {
    const exists = await this.client.indices.exists({ index: this.index });
    if (exists) return;
    try {
      await this.client.indices.create({ index: this.index, ...ordersIndexDefinition } as never);
    } catch (err) {
      if ((err as { body?: { error?: { type?: string } } }).body?.error?.type !== 'resource_already_exists_exception') {
        throw err;
      }
    }
  }

  async apply(event: OrderChangeEvent): Promise<ProjectionResult> {
    const document = toDocument(event);

    await this.client.index({
      index: this.index,
      id: event.aggregateId,
      document,
    });

    return { outcome: event.operation === 'DELETE' ? 'deleted' : 'applied' };
  }
}

export function toDocument(event: OrderChangeEvent): OrderDocument {
  if (event.operation === 'DELETE') {
    const last = event.before;
    return {
      order_id: event.aggregateId,
      status: last?.status ?? null,
      total_amount: last ? Number(last.totalAmount) : null,
      currency: last?.currency ?? null,
      version: event.version,
      is_deleted: true,
      created_at: last?.createdAt ?? null,
      updated_at: last?.updatedAt ?? null,
      deleted_at: event.occurredAt,
      last_event_id: event.eventId,
    };
  }
  const after = event.after!;
  return {
    order_id: after.id,
    status: after.status,
    total_amount: Number(after.totalAmount),
    currency: after.currency,
    version: event.version,
    is_deleted: false,
    created_at: after.createdAt,
    updated_at: after.updatedAt,
    deleted_at: null,
    last_event_id: event.eventId,
  };
}
