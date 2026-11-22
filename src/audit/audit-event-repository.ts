import type { PoolClient } from 'pg';

export type AuditEvent = {
  eventId: string;
  eventType: string;
  aggregateId: string;
  aggregateVersion: number;
  correlationId: string | null;
  payload: Record<string, unknown>;
};

export class AuditEventRepository {
  public constructor(private readonly client: PoolClient) {}

  public async append(event: AuditEvent): Promise<void> {
    await this.client.query(
      `
        INSERT INTO audit_events
          (event_id, event_type, aggregate_id, aggregate_version,
           correlation_id, payload)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb)
        ON CONFLICT (event_id) DO NOTHING
      `,
      [
        event.eventId,
        event.eventType,
        event.aggregateId,
        event.aggregateVersion,
        event.correlationId,
        JSON.stringify(event.payload),
      ],
    );
  }
}
