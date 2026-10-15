-- Run once, transactionally, against a database already initialized with 001.
BEGIN;

ALTER TABLE memory_outbox
 ADD COLUMN IF NOT EXISTS published_at timestamptz,
 ADD COLUMN IF NOT EXISTS publish_lease_until timestamptz,
 ADD COLUMN IF NOT EXISTS publish_retry_at timestamptz NOT NULL DEFAULT now(),
 ADD COLUMN IF NOT EXISTS publish_attempt integer NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS outbox_relay_due_idx ON memory_outbox
 (publish_retry_at,id)
 WHERE published_at IS NULL AND status IN ('pending','running');

CREATE INDEX IF NOT EXISTS outbox_scope_status_idx ON memory_outbox
 (tenant_id,user_id,status,id);

COMMIT;
