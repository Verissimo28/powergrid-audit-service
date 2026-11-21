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

  public constructor(
    private readonly rabbitUrl: string,
    private readonly exchange = 'powergrid.events',
    private readonly queue = 'audit.events',
  ) {}

  public async start(): Promise<void> {
    this.connection = await amqp.connect(this.rabbitUrl);
    this.channel = await this.connection.createChannel();

    await this.channel.assertExchange(this.exchange, 'topic', {
      durable: true,
    });

    await this.channel.assertQueue(this.queue, {
      durable: true,
    });

    await this.channel.bindQueue(this.queue, this.exchange, 'order.#');
    await this.channel.prefetch(10);

    await this.channel.consume(this.queue, (message) => {
      if (message === null) return;
      void this.handle(message);
    });
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
    await this.channel?.close();
    await this.connection?.close();
    this.channel = undefined;
    this.connection = undefined;
  }
}
