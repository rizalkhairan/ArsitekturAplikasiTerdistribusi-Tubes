-- Run once as the database owner on the Citus coordinator, after all groups join.
-- 48 shards is an implementation default, not an assignment requirement.
CREATE SCHEMA IF NOT EXISTS canonical;
-- statement
SET search_path = canonical, public;
-- statement
-- Recovery changes this row under an exclusive lock. Every writer holds a
-- shared row lock until COMMIT, so the acknowledgement policy cannot race a
-- change to Patroni's strict synchronous setting. Only administrators update it.
CREATE TABLE IF NOT EXISTS recovery (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    durability text NOT NULL CHECK (durability IN ('replicated', 'single_copy')),
    fenced_node text,
    changed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
-- statement
INSERT INTO recovery(singleton, durability) VALUES (true, 'replicated')
ON CONFLICT DO NOTHING;
-- statement
SET citus.shard_count = 48;
-- statement
SET citus.shard_replication_factor = 1;
-- statement
SELECT citus_set_node_property(nodename, nodeport, 'shouldhaveshards', true)
FROM pg_dist_node WHERE groupid = 0 AND noderole = 'primary';
-- statement

CREATE TABLE IF NOT EXISTS authorities (
    partition text PRIMARY KEY,
    epoch bigint NOT NULL CHECK (epoch > 0),
    owner text NOT NULL,
    expires_at timestamptz NOT NULL
);
-- statement
-- These rows intentionally stay on the replicated coordinator. Locking one row
-- serializes ownership installation with all mutations in that partition.
CREATE TABLE IF NOT EXISTS windows (
    partition text NOT NULL,
    window_id text NOT NULL,
    digest text NOT NULL,
    epoch bigint NOT NULL,
    receipt text NOT NULL,
    PRIMARY KEY (partition, window_id)
);
-- statement
CREATE TABLE IF NOT EXISTS events (
    event_key text PRIMARY KEY,
    hazard_id text NOT NULL,
    source text NOT NULL,
    source_ref_id text NOT NULL,
    partition text NOT NULL,
    version bigint NOT NULL CHECK (version > 0),
    operation text NOT NULL CHECK (operation IN ('upsert', 'retract')),
    digest text NOT NULL,
    event jsonb NOT NULL,
    ingested_at timestamptz NOT NULL,
    confirmed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
-- statement
SELECT create_distributed_table('events', 'event_key', colocate_with => 'none')
WHERE NOT EXISTS (SELECT 1 FROM pg_dist_partition WHERE logicalrelid = 'events'::regclass);
-- statement
CREATE INDEX IF NOT EXISTS events_hazard_id ON events (hazard_id);
-- statement
CREATE INDEX IF NOT EXISTS events_ingestion ON events (ingested_at, hazard_id);
-- statement
CREATE TABLE IF NOT EXISTS outbox (
    event_key text NOT NULL,
    version bigint NOT NULL,
    event_id text NOT NULL,
    hazard_id text NOT NULL,
    correlation_id text NOT NULL,
    topic text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz,
    PRIMARY KEY (event_key, version)
);
-- statement
SELECT create_distributed_table('outbox', 'event_key', colocate_with => 'events')
WHERE NOT EXISTS (SELECT 1 FROM pg_dist_partition WHERE logicalrelid = 'outbox'::regclass);
-- statement
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox (created_at) WHERE published_at IS NULL;
-- statement

-- Fixed, non-parameterized health probe; the application cannot run arbitrary
-- administrator commands. Strict Patroni sync mode must also stay enabled.
CREATE OR REPLACE FUNCTION canonical.replication_ready() RETURNS boolean
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
  SELECT count(*) = 3 AND bool_and(success AND result = 't')
  FROM pg_catalog.run_command_on_all_nodes($probe$
    SELECT current_setting('synchronous_standby_names') <> ''
      AND current_setting('synchronous_commit') = 'remote_apply'
      AND EXISTS (SELECT 1 FROM pg_stat_replication
                  WHERE state = 'streaming' AND sync_state IN ('sync', 'quorum'))
  $probe$);
$$;
-- statement
REVOKE ALL ON FUNCTION canonical.replication_ready() FROM PUBLIC;
-- statement
CREATE OR REPLACE FUNCTION canonical.primaries_ready() RETURNS boolean
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
  SELECT count(*) = 3 AND bool_and(success AND result = 't')
  FROM pg_catalog.run_command_on_all_nodes($probe$
    SELECT NOT pg_is_in_recovery()
  $probe$);
$$;
-- statement
REVOKE ALL ON FUNCTION canonical.primaries_ready() FROM PUBLIC;
-- statement
REVOKE ALL ON SCHEMA canonical FROM PUBLIC;
-- statement
GRANT USAGE ON SCHEMA canonical TO canonical_app;
-- statement
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA canonical TO canonical_app;
-- statement
REVOKE INSERT, UPDATE ON canonical.recovery FROM canonical_app;
-- statement
-- SELECT FOR SHARE requires UPDATE on at least one column. Granting the
-- immutable key alone lets writers lock the row, without changing the policy.
GRANT UPDATE(singleton) ON canonical.recovery TO canonical_app;
-- statement
GRANT EXECUTE ON FUNCTION canonical.replication_ready() TO canonical_app;
-- statement
GRANT EXECUTE ON FUNCTION canonical.primaries_ready() TO canonical_app;
-- statement
