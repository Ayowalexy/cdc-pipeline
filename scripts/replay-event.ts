/**
 * Controlled replay fixture: re-delivers the original Debezium record for one order version.
 *
 *   npm run replay:event -- --order-id ORD-1001 --version 2 [--operation UPDATE] [--times 1]
 *
 * Re-publishes the record bytes with the same key to the same partition (headers x-replay=controlled,
 * x-replay-of=<partition>/<offset>), then waits until the projection consumer has committed past it.
 * For a real broker replay (rewinding the consumer group) see TC-04 in scripts/run-scenarios.ts.
 */
import { esDoc, findOrderMessages, pgOrder, replayMessage, waitForCommitted } from './lib/local-env';

function arg(name: string): string | undefined {
  const i = process.argv.indexOf(`--${name}`);
  return i > 0 ? process.argv[i + 1] : undefined;
}

async function main() {
  const orderId = arg('order-id');
  const version = Number(arg('version'));
  const operation = arg('operation');
  const times = Number(arg('times') ?? 1);
  if (!orderId || !Number.isInteger(version)) {
    throw new Error(
      'usage: npm run replay:event -- --order-id <id> --version <n> [--operation CREATE|UPDATE|DELETE] [--times n]',
    );
  }

  const match = (await findOrderMessages(orderId)).find(
    (m) => m.event.version === version && (!operation || m.event.operation === operation),
  );
  if (!match) throw new Error(`no Debezium record found for ${orderId} v${version}${operation ? ` ${operation}` : ''}`);
  console.log(`original record: partition=${match.partition} offset=${match.offset} eventId=${match.event.eventId}`);

  for (let i = 0; i < times; i++) {
    const copy = await replayMessage(match);
    console.log(`re-delivered as partition=${copy.partition} offset=${copy.offset}`);
    await waitForCommitted(copy.partition, copy.offset);
  }
  console.log('projection consumer committed past the re-delivered record(s)');
  console.log({ postgres: await pgOrder(orderId), elasticsearch: (await esDoc(orderId)).source ?? null });
}

main().catch((err) => {
  console.error(err instanceof Error ? err.message : err);
  process.exit(1);
});
