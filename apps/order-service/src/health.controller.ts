import { Controller, Get, ServiceUnavailableException } from '@nestjs/common';
import { OrdersRepository } from './orders/orders.repository';

@Controller('health')
export class HealthController {
  constructor(private readonly orders: OrdersRepository) {}

  @Get()
  async check() {
    try {
      await this.orders.ping();
    } catch {
      throw new ServiceUnavailableException({ status: 'error', postgres: 'down' });
    }
    return { status: 'ok', postgres: 'up' };
  }
}
