import {
  BadRequestException,
  Body,
  ConflictException,
  Controller,
  Delete,
  Get,
  HttpCode,
  NotFoundException,
  Param,
  Patch,
  Post,
} from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { ORDER_STATUSES, OrderStatus } from '../../../../libs/contracts/order-change-event';
import { OrdersRepository } from './orders.repository';

const ORDER_ID = /^ORD-[A-Za-z0-9-]{1,60}$/;
const AMOUNT = /^\d{1,10}(\.\d{1,2})?$/;
const CURRENCY = /^[A-Z]{3}$/;

type CreateOrderBody = { id?: unknown; totalAmount?: unknown; currency?: unknown };
type UpdateStatusBody = { status?: unknown };

@Controller('orders')
export class OrdersController {
  constructor(private readonly orders: OrdersRepository) {}

  @Post()
  async create(@Body() body: CreateOrderBody = {}) {
    const id = body.id === undefined ? `ORD-${randomUUID()}` : body.id;
    if (typeof id !== 'string' || !ORDER_ID.test(id)) {
      throw new BadRequestException('id must match ORD-[A-Za-z0-9-]');
    }
    const totalAmount = body.totalAmount === undefined ? '0.00' : String(body.totalAmount);
    if (!AMOUNT.test(totalAmount)) {
      throw new BadRequestException('totalAmount must be a non-negative amount with at most 2 decimals');
    }
    const currency = body.currency === undefined ? null : body.currency;
    if (currency !== null && (typeof currency !== 'string' || !CURRENCY.test(currency))) {
      throw new BadRequestException('currency must be a 3-letter ISO code');
    }

    const created = await this.orders.insert({ id, status: 'PENDING', totalAmount, currency });
    if (!created) throw new ConflictException(`order ${id} already exists`);
    return created;
  }

  @Get(':id')
  async get(@Param('id') id: string) {
    const order = await this.orders.findById(id);
    if (!order) throw new NotFoundException(`order ${id} not found`);
    return order;
  }

  @Patch(':id/status')
  async updateStatus(@Param('id') id: string, @Body() body: UpdateStatusBody = {}) {
    if (!ORDER_STATUSES.includes(body.status as OrderStatus)) {
      throw new BadRequestException(`status must be one of ${ORDER_STATUSES.join(', ')}`);
    }
    const order = await this.orders.updateStatus(id, body.status as OrderStatus);
    if (!order) throw new NotFoundException(`order ${id} not found`);
    return order;
  }

  @Delete(':id')
  @HttpCode(200)
  async delete(@Param('id') id: string) {
    const deleted = await this.orders.delete(id);
    if (!deleted) throw new NotFoundException(`order ${id} not found`);
    return { deleted: true, order: deleted };
  }
}
