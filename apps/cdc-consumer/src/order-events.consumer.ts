import { LoggerService } from '@nestjs/common';
import { Consumer, EachMessagePayload, Kafka, Producer } from 'kafkajs';
import {
  EventNormalizationError,
  normalizeDebeziumOrderMessage,
} from '../../../libs/event-normalizer/debezium-order.adapter';
import { classifyEsError } from './es-errors';
import { OrderProjectionService, ProjectionResult } from './order-projection.service';

export type ConsumerConfig = {
  brokers: string[];
  clientId: string;
  groupId: string;
  topic: string;
  dlqTopic: string;
  /** Attempts per message for retryable (transient) errors before the consumer gives up. */
  maxAttempts: number;
  retryBaseMs: number;
  retryMaxMs: number;
  /** Restart the kafkajs consumer after a crash (production: true; tests set false to observe the crash). */
  restartOnFailure: boolean;
};

export class RetriesExhaustedError extends Error {}

/**
 * Delivery semantics: at-least-once.
 *  - autoCommit is disabled; the offset of a message is committed only after its outcome is
 *    established (applied / ignored_stale / duplicate / deleted, or written to the DLQ).
 *  - Retryable errors are retried in place (same partition is blocked, so per-order ordering is
 *    preserved). If retries are exhausted the message is NOT committed and the error propagates,
 *    so kafkajs restarts the consumer and the message is fetched again from the last committed offset.
 *  - A crash after the Elasticsearch write but before the commit causes redelivery; the projection
 *    must therefore be idempotent for re-applied events.
 */
export class OrderEventsConsumer {
  private readonly kafka: Kafka;
  private consumer?: Consumer;
  private producer?: Producer;
  private running = false;

  constructor(
    private readonly config: ConsumerConfig,
    private readonly projection: Pick<OrderProjectionService, 'apply'>,
    private readonly logger: LoggerService,
  ) {
    this.kafka = new Kafka({ clientId: config.clientId, brokers: config.brokers, logLevel: 1 });
  }

  get isRunning() {
    return this.running;
  }

  async start(): Promise<void> {
    this.producer = this.kafka.producer();
    await this.producer.connect();
    this.consumer = this.kafka.consumer({
      groupId: this.config.groupId,
      retry: { retries: this.config.restartOnFailure ? 5 : 0, restartOnFailure: async () => this.config.restartOnFailure },
    });
    this.consumer.on(this.consumer.events.CRASH, (e) => {
      this.running = this.config.restartOnFailure && e.payload.restart;
      this.logger.error({ msg: 'consumer crashed', error: String(e.payload.error), restart: e.payload.restart });
    });
    this.consumer.on(this.consumer.events.GROUP_JOIN, (e) => {
      this.logger.log({ msg: 'consumer joined group', groupId: e.payload.groupId, assignment: e.payload.memberAssignment });
    });
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: this.config.topic, fromBeginning: true });
    await this.consumer.run({ autoCommit: false, eachMessage: (payload) => this.handle(payload) });
    this.running = true;
  }

  async stop(): Promise<void> {
    this.running = false;
    await this.consumer?.disconnect();
    await this.producer?.disconnect();
  }

  private async handle({ topic, partition, message, heartbeat }: EachMessagePayload): Promise<void> {
    const position = { partition, offset: message.offset };
    const correlationId = `${topic}/${partition}/${message.offset}`;
    const replayHeader = message.headers?.['x-replay']?.toString();

    let normalized;
    try {
      normalized = normalizeDebeziumOrderMessage(message.key, message.value, position);
    } catch (err) {
      if (!(err instanceof EventNormalizationError)) throw err;
      await this.deadLetter(topic, partition, message.offset, message.key, message.value, err.message);
      this.logger.error({
        msg: 'order event',
        outcome: 'permanent_error',
        correlationId,
        error: err.message,
        dlqTopic: this.config.dlqTopic,
      });
      await this.commit(topic, partition, message.offset);
      return;
    }

    if (normalized.kind === 'kafka_tombstone') {
      // Compaction marker that follows a Debezium delete event; the delete itself was handled.
      this.logger.log({ msg: 'kafka tombstone skipped', orderId: normalized.aggregateId, correlationId });
      await this.commit(topic, partition, message.offset);
      return;
    }

    const event = normalized.event;
    const base = {
      msg: 'order event',
      eventId: event.eventId,
      orderId: event.aggregateId,
      operation: event.operation,
      incomingVersion: event.version,
      sourcePosition: event.sourcePosition,
      correlationId,
      ...(replayHeader ? { replay: replayHeader } : {}),
    };

    let result: ProjectionResult | undefined;
    for (let attempt = 1; result === undefined; attempt++) {
      try {
        result = await this.projection.apply(event);
      } catch (err) {
        const errorClass = classifyEsError(err);
        if (errorClass === 'permanent') {
          await this.deadLetter(topic, partition, message.offset, message.key, message.value, String(err));
          this.logger.error({ ...base, outcome: 'permanent_error', error: String(err), dlqTopic: this.config.dlqTopic });
          await this.commit(topic, partition, message.offset);
          return;
        }
        this.logger.warn({ ...base, outcome: 'retryable_error', attempt, maxAttempts: this.config.maxAttempts, error: String(err) });
        if (attempt >= this.config.maxAttempts) {
          // Not committed: the message will be re-fetched from the last committed offset.
          throw new RetriesExhaustedError(`gave up on ${correlationId} after ${attempt} attempts`);
        }
        await sleep(Math.min(this.config.retryBaseMs * 2 ** (attempt - 1), this.config.retryMaxMs));
        await heartbeat();
      }
    }

    this.logger.log({ ...base, outcome: result.outcome, indexedVersion: result.indexedVersion });
    await this.commit(topic, partition, message.offset);
  }

  private async commit(topic: string, partition: number, offset: string) {
    await this.consumer!.commitOffsets([{ topic, partition, offset: (BigInt(offset) + 1n).toString() }]);
  }

  private async deadLetter(
    topic: string,
    partition: number,
    offset: string,
    key: Buffer | null,
    value: Buffer | null,
    reason: string,
  ) {
    await this.producer!.send({
      topic: this.config.dlqTopic,
      messages: [
        {
          key,
          value,
          headers: { 'x-source-topic': topic, 'x-source-partition': String(partition), 'x-source-offset': offset, 'x-error': reason },
        },
      ],
    });
  }
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
