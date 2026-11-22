import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import test from 'node:test';
import type {
  Channel,
  ChannelModel,
  ConsumeMessage,
  Options,
} from 'amqplib';
process.env.AUDIT_DATABASE_URL ??=
  'postgresql://test:test@localhost:5432/test';

const { AuditConsumer } = await import(
  '../../src/messaging/audit-consumer.js'
);

class FakeChannel {
  public consumeCalls = 0;

  public async assertExchange(
    _exchange: string,
    _type: string,
    _options: Options.AssertExchange,
  ): Promise<void> {}

  public async assertQueue(
    _queue: string,
    _options: Options.AssertQueue,
  ): Promise<void> {}

  public async bindQueue(
    _queue: string,
    _exchange: string,
    _routingKey: string,
  ): Promise<void> {}

  public async prefetch(_count: number): Promise<void> {}

  public async consume(
    _queue: string,
    _handler: (message: ConsumeMessage | null) => void,
  ): Promise<{ consumerTag: string }> {
    this.consumeCalls += 1;
    return { consumerTag: `consumer-${this.consumeCalls}` };
  }

  public ack(_message: ConsumeMessage): void {}

  public nack(
    _message: ConsumeMessage,
    _allUpTo?: boolean,
    _requeue?: boolean,
  ): void {}

  public async close(): Promise<void> {}
}

class FakeConnection extends EventEmitter {
  public readonly channel = new FakeChannel();

  public async createChannel(): Promise<Channel> {
    return this.channel as unknown as Channel;
  }

  public async close(): Promise<void> {}
}

function wait(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

async function waitFor(
  condition: () => boolean,
  timeoutMilliseconds = 4000,
): Promise<void> {
  const startedAt = Date.now();

  while (!condition()) {
    if (Date.now() - startedAt >= timeoutMilliseconds) {
      throw new Error('Tempo limite aguardando condição do teste.');
    }

    await wait(25);
  }
}

function createConsumer(
  connectRabbit: (url: string) => Promise<ChannelModel>,
): AuditConsumer {
  return new AuditConsumer(
    'amqp://fake',
    'powergrid.events',
    'audit.events',
    connectRabbit,
  );
}

test('reconecta e recria o consumidor após fechamento da conexão', async () => {
  const connections: FakeConnection[] = [];
  let connectCalls = 0;

  const connectRabbit = async (): Promise<ChannelModel> => {
    connectCalls += 1;

    const connection = new FakeConnection();
    connections.push(connection);

    return connection as unknown as ChannelModel;
  };

  const consumer = createConsumer(connectRabbit);

  await consumer.start();

  assert.equal(connectCalls, 1);
  assert.equal(connections[0]?.channel.consumeCalls, 1);

  connections[0]?.emit('close');

  await wait(100);

  assert.equal(connectCalls, 2);
  assert.equal(connections[1]?.channel.consumeCalls, 1);

  await consumer.close();
});

test('não cria múltiplos loops para eventos close repetidos', async () => {
  const connections: FakeConnection[] = [];
  let connectCalls = 0;

  const connectRabbit = async (): Promise<ChannelModel> => {
    connectCalls += 1;

    const connection = new FakeConnection();
    connections.push(connection);

    return connection as unknown as ChannelModel;
  };

  const consumer = createConsumer(connectRabbit);

  await consumer.start();

  connections[0]?.emit('close');
  connections[0]?.emit('close');

  await wait(100);

  assert.equal(connectCalls, 2);
  assert.equal(connections[1]?.channel.consumeCalls, 1);

  await consumer.close();
});

test('tenta novamente após falha e reconecta depois do backoff', async () => {
  const connections: FakeConnection[] = [];
  let connectCalls = 0;

  const connectRabbit = async (): Promise<ChannelModel> => {
    connectCalls += 1;

    if (connectCalls === 2) {
      throw new Error('RabbitMQ indisponível durante a reconexão.');
    }

    const connection = new FakeConnection();
    connections.push(connection);

    return connection as unknown as ChannelModel;
  };

  const consumer = createConsumer(connectRabbit);

  await consumer.start();

  assert.equal(connectCalls, 1);

  connections[0]?.emit('close');

  await waitFor(() => connectCalls >= 2);
  assert.equal(connectCalls, 2);
  assert.equal(connections.length, 1);

  await waitFor(() => connectCalls >= 3);

  assert.equal(connectCalls, 3);
  assert.equal(connections.length, 2);
  assert.equal(connections[1]?.channel.consumeCalls, 1);

  await consumer.close();
});

test('não reconecta após close explícito do consumidor', async () => {
  const connections: FakeConnection[] = [];
  let connectCalls = 0;

  const connectRabbit = async (): Promise<ChannelModel> => {
    connectCalls += 1;

    const connection = new FakeConnection();
    connections.push(connection);

    return connection as unknown as ChannelModel;
  };

  const consumer = createConsumer(connectRabbit);

  await consumer.start();
  await consumer.close();

  connections[0]?.emit('close');

  await wait(1100);

  assert.equal(connectCalls, 1);
});
