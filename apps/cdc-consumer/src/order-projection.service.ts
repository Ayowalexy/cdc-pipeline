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

  /**
   * Version-guarded write. The PostgreSQL row version is used as the Elasticsearch external
   * version, so the shard itself performs the compare-and-write atomically:
   *   incoming > stored  -> written            (applied / deleted)
   *   incoming == stored -> 409, not written   (duplicate)
   *   incoming < stored  -> 409, not written   (ignored_stale)
   * No read-then-write in application code. Tombstones keep their version, so stale updates
   * cannot resurrect a deleted order.
   */
  async apply(event: OrderChangeEvent): Promise<ProjectionResult> {
    const document = toDocument(event);

    try {
      await this.client.index({
        index: this.index,
        id: event.aggregateId,
        document,
        version: event.version,
        version_type: 'external',
      });
    } catch (err) {
      const indexedVersion = versionConflictCurrentVersion(err);
      if (indexedVersion === undefined) throw err; // not a version conflict: classified by the consumer
      return { outcome: classifyVersionConflict(event.version, indexedVersion), indexedVersion };
    }

    return { outcome: event.operation === 'DELETE' ? 'deleted' : 'applied' };
  }
}

/** Outcome of a rejected external-version write, given the version Elasticsearch already holds. */
export function classifyVersionConflict(
  incomingVersion: number,
  indexedVersion: number,
): 'duplicate' | 'ignored_stale' {
  return incomingVersion === indexedVersion ? 'duplicate' : 'ignored_stale';
}

/**
 * Returns the stored version when `err` is an Elasticsearch version conflict, otherwise undefined.
 * Only a 409 `version_conflict_engine_exception` is treated as an expected outcome; mapping or
 * infrastructure errors keep propagating.
 */
export function versionConflictCurrentVersion(err: unknown): number | undefined {
  const e = err as { statusCode?: number; body?: { error?: { type?: string; reason?: string } } };
  if (e?.statusCode !== 409 || e.body?.error?.type !== 'version_conflict_engine_exception') return undefined;
  // e.g. "[ORD-1]: version conflict, current version [4] is higher or equal to the one provided [2]"
  const match = /current version \[(\d+)\]/.exec(e.body.error.reason ?? '');
  return match ? Number(match[1]) : undefined;
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
