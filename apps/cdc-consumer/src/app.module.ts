import { Client } from '@elastic/elasticsearch';
import {
  BeforeApplicationShutdown,
  Controller,
  Get,
  Inject,
  Logger,
  Module,
  OnApplicationBootstrap,
  OnApplicationShutdown,
  ServiceUnavailableException,
} from '@nestjs/common';
import { OrderEventsConsumer } from './order-events.consumer';
import { OrderProjectionService } from './order-projection.service';

const ES_CLIENT = Symbol('ES_CLIENT');

@Controller('health')
class HealthController {
  constructor(
    @Inject(ES_CLIENT) private readonly es: Client,
    private readonly consumer: OrderEventsConsumer,
  ) {}

  @Get()
  async check() {
    const elasticsearch = await this.es.ping().then(
      () => 'up',
      () => 'down',
    );
    const kafkaConsumer = this.consumer.isRunning ? 'running' : 'stopped';
    const body = {
      status: elasticsearch === 'up' && kafkaConsumer === 'running' ? 'ok' : 'error',
      elasticsearch,
      kafkaConsumer,
    };
    if (body.status !== 'ok') throw new ServiceUnavailableException(body);
    return body;
  }
}

@Module({
  controllers: [HealthController],
  providers: [
    {
      provide: ES_CLIENT,
      useFactory: () => new Client({ node: process.env.ELASTICSEARCH_URL ?? 'http://localhost:9200' }),
    },
    {
      provide: OrderProjectionService,
      inject: [ES_CLIENT],
      useFactory: (es: Client) => new OrderProjectionService(es, process.env.ES_ORDERS_INDEX ?? 'orders'),
    },
    {
      provide: OrderEventsConsumer,
      inject: [OrderProjectionService],
      useFactory: (projection: OrderProjectionService) =>
        new OrderEventsConsumer(
          {
            brokers: (process.env.KAFKA_BROKERS ?? 'localhost:29092').split(','),
            clientId: 'cdc-consumer',
            groupId: process.env.KAFKA_GROUP_ID ?? 'order-projection',
            topic: process.env.KAFKA_ORDERS_TOPIC ?? 'cdc.public.orders',
            dlqTopic: process.env.KAFKA_DLQ_TOPIC ?? 'cdc.public.orders.dlq',
            maxAttempts: Number(process.env.CONSUMER_MAX_ATTEMPTS ?? 8),
            retryBaseMs: 250,
            retryMaxMs: 5000,
            restartOnFailure: true,
          },
          projection,
          new Logger('OrderEventsConsumer'),
        ),
    },
  ],
})
export class AppModule implements OnApplicationBootstrap, BeforeApplicationShutdown, OnApplicationShutdown {
  private readonly logger = new Logger('AppModule');

  constructor(
    private readonly projection: OrderProjectionService,
    private readonly consumer: OrderEventsConsumer,
    @Inject(ES_CLIENT) private readonly es: Client,
  ) {}

  async onApplicationBootstrap() {
    await this.projection.ensureIndex();
    await this.consumer.start();
  }

  /** Runs before the HTTP server closes: leave the consumer group promptly so a restart or offset reset is not blocked. */
  async beforeApplicationShutdown(signal?: string) {
    this.logger.log({ msg: 'shutdown: stopping Kafka consumer', signal });
    await this.consumer.stop();
    this.logger.log({ msg: 'shutdown: Kafka consumer disconnected (left group)' });
  }

  async onApplicationShutdown() {
    await this.es.close();
  }
}
