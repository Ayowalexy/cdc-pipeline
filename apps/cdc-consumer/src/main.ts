import 'reflect-metadata';
import { ConsoleLogger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule, {
    logger: new ConsoleLogger({ json: true, context: 'cdc-consumer' }),
  });
  app.enableShutdownHooks();
  const port = Number(process.env.CDC_CONSUMER_PORT ?? 3101);
  await app.listen(port, '0.0.0.0');
}

void bootstrap();
