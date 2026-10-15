CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS memory_events (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 tenant_id text NOT NULL,
 user_id text NOT NULL,
 agent_id text NOT NULL DEFAULT 'phone_robot',
 robot_id text NOT NULL DEFAULT '',
 session_id text NOT NULL,
 source text NOT NULL CHECK (source IN ('user','agent','tool','sensor','system')),
 content text NOT NULL,
 content_sha256 text NOT NULL,
 idempotency_key text,
 occurred_at timestamptz NOT NULL DEFAULT now(),
 recorded_at timestamptz NOT NULL DEFAULT now(),
 deleted_at timestamptz,
 UNIQUE (tenant_id,user_id,idempotency_key)
);
CREATE INDEX IF NOT EXISTS events_session_idx ON memory_events (tenant_id,user_id,session_id,recorded_at DESC) WHERE deleted_at IS NULL;
CREATE TABLE IF NOT EXISTS memory_episodes (
 event_id uuid PRIMARY KEY REFERENCES memory_events(id) ON DELETE CASCADE,
 tenant_id text NOT NULL,
 user_id text NOT NULL,
 occurred_at timestamptz NOT NULL,
 content text NOT NULL,
 lexemes tsvector GENERATED ALWAYS AS (to_tsvector('simple', content)) STORED,
 embedding vector(768),
 expires_at timestamptz,
 deleted_at timestamptz
);
CREATE INDEX IF NOT EXISTS episodes_lexemes_idx ON memory_episodes USING gin(lexemes);
CREATE INDEX IF NOT EXISTS episodes_scope_idx ON memory_episodes(tenant_id,user_id,occurred_at DESC);
CREATE INDEX IF NOT EXISTS episodes_embedding_idx ON memory_episodes USING hnsw (embedding vector_cosine_ops) WHERE embedding IS NOT NULL AND deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS memory_facts (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 tenant_id text NOT NULL,
 user_id text NOT NULL,
 subject text NOT NULL,
 predicate text NOT NULL,
 value jsonb NOT NULL,
 confidence real NOT NULL CHECK(confidence BETWEEN 0 AND 1),
 source_event_id uuid NOT NULL REFERENCES memory_events(id),
 version integer NOT NULL,
 valid_from timestamptz NOT NULL,
 valid_until timestamptz,
 recorded_from timestamptz NOT NULL DEFAULT now(),
 recorded_until timestamptz,
 asserted_by text NOT NULL CHECK(asserted_by IN ('user_explicit','deterministic_extract','inference','tool_verified')),
 CHECK (valid_until IS NULL OR valid_until >= valid_from),
 CHECK (recorded_until IS NULL OR recorded_until >= recorded_from)
);
-- Current knowledge may have multiple non-overlapping valid-time segments.
ALTER TABLE memory_facts ADD CONSTRAINT no_overlapping_current_validity
 EXCLUDE USING gist (
  tenant_id WITH =, user_id WITH =, subject WITH =, predicate WITH =,
  tstzrange(valid_from,COALESCE(valid_until,'infinity'::timestamptz),'[)') WITH &&
 ) WHERE (recorded_until IS NULL);
CREATE INDEX IF NOT EXISTS facts_temporal_idx ON memory_facts(tenant_id,user_id,valid_from,valid_until,recorded_from,recorded_until);
CREATE INDEX IF NOT EXISTS facts_subject_idx ON memory_facts(tenant_id,user_id,subject,predicate,recorded_from DESC);

CREATE TABLE IF NOT EXISTS memory_procedures (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 tenant_id text NOT NULL,
 user_id text NOT NULL,
 name text NOT NULL,
 version integer NOT NULL DEFAULT 1,
 steps jsonb NOT NULL,
 required_permissions text[] NOT NULL DEFAULT '{}',
 state text NOT NULL CHECK (state IN ('proposed','approved','disabled')),
 created_at timestamptz NOT NULL DEFAULT now(),
 approved_at timestamptz,
 UNIQUE(tenant_id,user_id,name,version)
);
CREATE TABLE IF NOT EXISTS memory_outbox (
 id bigserial PRIMARY KEY,
 event_type text NOT NULL,
 tenant_id text NOT NULL,
 user_id text NOT NULL,
 aggregate_id uuid,
 payload jsonb NOT NULL DEFAULT '{}'::jsonb,
 attempt integer NOT NULL DEFAULT 0,
 status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','running','done','dead')),
 available_at timestamptz NOT NULL DEFAULT now(),
 lease_until timestamptz,
 last_error text,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS outbox_pending_idx ON memory_outbox(status,available_at,id) WHERE status IN ('pending','running');
CREATE TABLE IF NOT EXISTS memory_deletions (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 tenant_id text NOT NULL,
 user_id text NOT NULL,
 requested_at timestamptz NOT NULL DEFAULT now(),
 projections_complete_at timestamptz
);
CREATE INDEX IF NOT EXISTS deletion_scope_idx ON memory_deletions(tenant_id,user_id,requested_at DESC);

-- Real deployments must use non-superuser service roles. The Compose database
-- owner bypasses RLS; FORCE RLS on these tables requires separate worker roles.
DO $$
DECLARE t text;
BEGIN
 FOREACH t IN ARRAY ARRAY['memory_events','memory_episodes','memory_facts','memory_procedures'] LOOP
  EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
  EXECUTE format('DROP POLICY IF EXISTS scoped_user ON %I',t);
  EXECUTE format('CREATE POLICY scoped_user ON %I USING (tenant_id = current_setting(''app.tenant_id'',true) AND user_id = current_setting(''app.user_id'',true)) WITH CHECK (tenant_id = current_setting(''app.tenant_id'',true) AND user_id = current_setting(''app.user_id'',true))',t);
 END LOOP;
END $$;
