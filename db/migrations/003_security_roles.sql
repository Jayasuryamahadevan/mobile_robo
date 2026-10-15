-- Security boundary for non-owner runtime roles.
-- These NOLOGIN roles intentionally contain NO embedded credentials.
-- Provision LOGIN and rotated passwords through your secret-management system.
BEGIN;
DO $$
BEGIN
 IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='memory_api') THEN
  CREATE ROLE memory_api NOLOGIN NOINHERIT;
 END IF;
 IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='memory_projector') THEN
  CREATE ROLE memory_projector NOLOGIN NOINHERIT BYPASSRLS;
 END IF;
END
$$;
DO $$
BEGIN
 EXECUTE format('GRANT CONNECT ON DATABASE %I TO memory_api,memory_projector',current_database());
END
$$;
GRANT USAGE ON SCHEMA public TO memory_api,memory_projector;
GRANT SELECT,INSERT,UPDATE,DELETE ON
 memory_events,memory_episodes,memory_facts,memory_procedures
 TO memory_api;
GRANT SELECT,INSERT,DELETE ON memory_outbox TO memory_api;
GRANT SELECT,INSERT ON memory_deletions TO memory_api;
GRANT SELECT,INSERT,UPDATE,DELETE ON
 memory_events,memory_episodes,memory_facts,memory_procedures,memory_outbox,memory_deletions
 TO memory_projector;
GRANT USAGE,SELECT ON SEQUENCE memory_outbox_id_seq TO memory_api,memory_projector;

ALTER TABLE memory_outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory_deletions ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS scoped_user ON memory_outbox;
DROP POLICY IF EXISTS scoped_user ON memory_deletions;
CREATE POLICY scoped_user ON memory_outbox
 USING (tenant_id=current_setting('app.tenant_id',true)
 AND user_id=current_setting('app.user_id',true))
 WITH CHECK (tenant_id=current_setting('app.tenant_id',true)
 AND user_id=current_setting('app.user_id',true));
CREATE POLICY scoped_user ON memory_deletions
 USING (tenant_id=current_setting('app.tenant_id',true)
 AND user_id=current_setting('app.user_id',true))
 WITH CHECK (tenant_id=current_setting('app.tenant_id',true)
 AND user_id=current_setting('app.user_id',true));
COMMIT;
