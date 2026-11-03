import { Inject, Injectable } from '@nestjs/common';
import { Pool } from 'pg';
import { OrderSnapshot, OrderStatus } from '../../../../libs/contracts/order-change-event';

export const PG_POOL = Symbol('PG_POOL');

type OrderRow = {
  id: string;
  status: OrderStatus;
  total_amount: string;
  currency: string | null;
  version: number;
  created_at: Date;
  updated_at: Date;
};

const COLUMNS = 'id, status, total_amount, currency, version, created_at, updated_at';

@Injectable()
export class OrdersRepository {
  constructor(@Inject(PG_POOL) private readonly pool: Pool) {}

  async insert(order: { id: string; status: OrderStatus; totalAmount: string; currency: string | null }) {
    const { rows } = await this.pool.query<OrderRow>(
      `INSERT INTO orders (id, status, total_amount, currency) VALUES ($1, $2, $3, $4)
       ON CONFLICT (id) DO NOTHING RETURNING ${COLUMNS}`,
      [order.id, order.status, order.totalAmount, order.currency],
    );
    return rows[0] ? toSnapshot(rows[0]) : null;
  }

  async findById(id: string) {
    const { rows } = await this.pool.query<OrderRow>(`SELECT ${COLUMNS} FROM orders WHERE id = $1`, [id]);
    return rows[0] ? toSnapshot(rows[0]) : null;
  }

  /**
   * Single-statement update: the row lock taken by UPDATE makes `version = version + 1` atomic
   * under concurrent requests. A request that does not change the status is not a state change
   * and does not bump the version.
   */
  async updateStatus(id: string, status: OrderStatus) {
    const { rows } = await this.pool.query<OrderRow>(
      `UPDATE orders SET status = $2, version = version + 1, updated_at = now()
       WHERE id = $1 AND status <> $2 RETURNING ${COLUMNS}`,
      [id, status],
    );
    return rows[0] ? toSnapshot(rows[0]) : this.findById(id);
  }

  async delete(id: string) {
    const { rows } = await this.pool.query<OrderRow>(`DELETE FROM orders WHERE id = $1 RETURNING ${COLUMNS}`, [id]);
    return rows[0] ? toSnapshot(rows[0]) : null;
  }

  async ping() {
    await this.pool.query('SELECT 1');
  }
}

function toSnapshot(row: OrderRow): OrderSnapshot {
  return {
    id: row.id,
    status: row.status,
    totalAmount: row.total_amount,
    currency: row.currency,
    version: row.version,
    createdAt: row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString(),
  };
}
