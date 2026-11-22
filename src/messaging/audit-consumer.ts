import amqp, {
  type Channel,
  type ChannelModel,
  type ConsumeMessage,
} from 'amqplib';
import { auditPool } from '../database/pool.js';
import { AuditEventRepository } from '../audit/audit-event-repository.js';

type IncomingEvent = {
  eventId: string;
  eventType: string;
  aggregateId: string;
  aggregateVersion: number;
  correlationId?: string;
  producer?: string;
  [key: string]: unknown;
};

function parseEvent(message: ConsumeMessage): IncomingEvent {
  const parsed: unknown = JSON.parse(message.content.toString('utf8'));

  if (typeof parsed !== 'object' || parsed === null) {
    throw new Error('Evento RabbitMQ inválido.');
  }

  const event = parsed as Partial<IncomingEvent>;

  if (
    typeof event.eventId !== 'string' ||
    typeof event.eventType !== 'string' ||
    typeof event.aggregateId !== 'string' ||
    typeof event.aggregateVersion !== 'number'
  ) {
    throw new Error('Evento sem envelope obrigatório.');
  }

  return event as IncomingEvent;
}

export class AuditConsumer {
  private connection: ChannelModel | undefined;
  private channel: Channel | undefined;
  private stopping = false;
  private reconnecting = false;

  public constructor(
    private readonly rabbitUrl: string,
    private readonly exchange = 'powergrid.events',
    private readonly queue = 'audit.events',
  ) {}

  public async start(): Promise<void> {
    this.stopping = false;
    await this.connect();

    console.log(
      JSON.stringify({
        service: 'audit-service',
        action: 'rabbit-connected',
      }),
    );
  }

  private async connect(): Promise<void> {
    const connection = await amqp.connect(this.rabbitUrl);
    const channel = await connection.createChannel();

    this.connection = connection;
    this.channel = channel;

    connection.on('close', () => {
      if (!this.stopping) {
        this.scheduleReconnect();
      }
    });

    connection.on('error', (error) => {
      if (!this.stopping) {
        console.error('Conexão RabbitMQ do Audit apresentou erro', error);
      }
    });

    await channel.assertExchange(this.exchange, 'topic', {
      durable: true,
    });

    await channel.assertQueue(this.queue, {
      durable: true,
    });

    await channel.bindQueue(this.queue, this.exchange, 'order.#');
    await channel.prefetch(10);

    await channel.consume(this.queue, (message) => {
      if (message === null) return;
      void this.handle(message);
    });
  }

  private scheduleReconnect(): void {
    if (this.reconnecting || this.stopping) {
      return;
    }

    this.reconnecting = true;

    void (async () => {
      let delay = 1000;

      while (!this.stopping) {
        try {
          await this.connect();

          console.log(
            JSON.stringify({
              service: 'audit-service',
              action: 'rabbit-reconnected',
            }),
          );

          this.reconnecting = false;
          return;
        } catch (error) {
          console.error(
            JSON.stringify({
              service: 'audit-service',
              action: 'rabbit-reconnect-failed',
              error: error instanceof Error ? error.message : error,
              retryInMs: delay,
            }),
          );

          await new Promise((resolve) => setTimeout(resolve, delay));
          delay = Math.min(delay * 2, 30000);
        }
      }

      this.reconnecting = false;
    })();
  }

  private async handle(message: ConsumeMessage): Promise<void> {
    const channel = this.channel;
    if (channel === undefined) return;

    try {
      const event = parseEvent(message);
      const client = await auditPool.connect();

      try {
        await client.query('BEGIN');
        await new AuditEventRepository(client).append({
          eventId: event.eventId,
          eventType: event.eventType,
          aggregateId: event.aggregateId,
          aggregateVersion: event.aggregateVersion,
          correlationId:
            typeof event.correlationId === 'string'
              ? event.correlationId
              : null,
          payload: event,
        });
        await client.query('COMMIT');
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      } finally {
        client.release();
      }

      channel.ack(message);
    } catch (error) {
      console.error('Falha ao processar evento de auditoria', error);
      channel.nack(message, false, true);
    }
  }

  public async close(): Promise<void> {
    this.stopping = true;
    this.reconnecting = false;

    await this.channel?.close().catch(() => undefined);
    await this.connection?.close().catch(() => undefined);

    this.channel = undefined;
    this.connection = undefined;
  }
}
