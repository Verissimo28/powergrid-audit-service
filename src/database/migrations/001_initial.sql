CREATE TABLE IF NOT EXISTS audit_events (
  event_id UUID PRIMARY KEY,
  event_type TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  aggregate_version INTEGER NOT NULL,
  correlation_id TEXT,
  payload JSONB NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS audit_events_aggregate_idx
  ON audit_events (aggregate_id, aggregate_version);

CREATE INDEX IF NOT EXISTS audit_events_type_idx
  ON audit_events (event_type, received_at);
