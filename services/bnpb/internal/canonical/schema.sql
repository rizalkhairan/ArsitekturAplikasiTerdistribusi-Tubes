-- Run once as the database owner on the Citus coordinator, after all groups join.
-- 12 shards is the portable default; RF2 still gives two active copies.
CREATE SCHEMA IF NOT EXISTS canonical;
-- statement
SET search_path = canonical, public;
-- statement
-- Recovery changes this row under an exclusive lock. Every writer holds a
-- shared row lock until COMMIT, so the acknowledgement policy cannot race a
-- change to the active durability policy. Only administrators update it.
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
SET citus.shard_count = 12;
-- statement
SET citus.shard_replication_factor = 2;
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
-- The poller resolves every upstream record by source_ref_id before writing;
-- without this, each lookup is a full distributed scan.
CREATE INDEX IF NOT EXISTS events_source_ref ON events (source_ref_id);
-- statement
CREATE INDEX IF NOT EXISTS events_ingestion ON events (ingested_at, hazard_id);
-- statement
CREATE INDEX IF NOT EXISTS events_source_occurred ON events
    (source, (event->>'occurred_at') DESC, hazard_id) WHERE operation='upsert';
-- statement
CREATE INDEX IF NOT EXISTS events_occurred ON events
    ((event->>'occurred_at') DESC, hazard_id) WHERE operation='upsert';
-- statement
CREATE INDEX IF NOT EXISTS events_severity ON events
    ((event->>'severity')) WHERE operation='upsert';
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
-- administrator commands. Citus placement health gates the result.
CREATE OR REPLACE FUNCTION canonical.replication_ready() RETURNS boolean
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
  -- Lean Citus mode: durability is provided by Citus shard replication, not
  -- streaming standbys. Every distributed shard must have two active copies.
  SELECT (SELECT count(*) FROM pg_dist_node WHERE noderole = 'primary' AND isactive) >= 3
     AND NOT EXISTS (
       SELECT 1 FROM pg_dist_shard s
       WHERE (SELECT count(*) FROM pg_dist_placement p JOIN pg_dist_node n USING(groupid)
              WHERE p.shardid = s.shardid AND p.shardstate = 1 AND n.isactive) < 2
     );
$$;
-- statement
REVOKE ALL ON FUNCTION canonical.replication_ready() FROM PUBLIC;
-- statement
CREATE OR REPLACE FUNCTION canonical.primaries_ready() RETURNS boolean
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
  SELECT (SELECT count(*) FROM pg_dist_node WHERE noderole = 'primary' AND isactive) >= 2
     AND NOT EXISTS (
       SELECT 1 FROM pg_dist_shard s
       WHERE NOT EXISTS (
         SELECT 1 FROM pg_dist_placement p JOIN pg_dist_node n USING(groupid)
         WHERE p.shardid = s.shardid AND p.shardstate = 1 AND n.isactive
       )
     );
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
-- Read-only Citus metadata is needed for the Audit API location and cursor
-- coverage checks. The application cannot modify routing or placement.
GRANT SELECT ON pg_dist_node, pg_dist_placement, pg_dist_shard TO canonical_app;
-- statement

-- Immutable query cache is local to the replicated coordinator, shared by all
-- Aggregators. It is disposable: a coordinator replacement may lose an
-- asynchronously saved snapshot, in which case its cursor is explicitly rejected.
CREATE TABLE IF NOT EXISTS canonical.read_snapshots (
    id uuid PRIMARY KEY,
    secret bytea NOT NULL,
    filters jsonb NOT NULL,
    items jsonb NOT NULL,
    missing jsonb NOT NULL,
    expires_at timestamptz NOT NULL
);
-- statement
GRANT SELECT, INSERT, DELETE ON canonical.read_snapshots TO canonical_app;
-- statement

-- The application gets a fixed read operation, never arbitrary worker SQL or
-- administrator credentials. Metadata SHARE locks prevent routing/placement
-- cutover while one shard is being observed; ordinary event writes continue.
CREATE OR REPLACE FUNCTION canonical.read_shard(wanted bigint, source_filter text, since_filter text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp SET lock_timeout = '1000ms'
SET statement_timeout = '5000ms' SET citus.node_connection_timeout = '3000ms'
AS $$
DECLARE
    placement record;
    response record;
    command text;
    payload jsonb;
BEGIN
    LOCK TABLE pg_dist_shard, pg_dist_placement, pg_dist_node IN SHARE MODE;
    IF NOT EXISTS (SELECT 1 FROM pg_dist_shard WHERE shardid=wanted
                   AND logicalrelid='canonical.events'::regclass) THEN
        RAISE EXCEPTION 'unknown event shard';
    END IF;
    command := format($cmd$
      WITH records AS MATERIALIZED (
        SELECT jsonb_build_object('version',version,'operation',operation,'event',event) AS value
        FROM %s WHERE (%L='' OR source=%L)
          AND (%L='' OR ingested_at >= NULLIF(%L,'')::timestamptz)
        LIMIT 10001
      ), bounds AS (SELECT count(*) AS n,coalesce(sum(octet_length(value::text)),0) AS bytes FROM records)
      SELECT CASE WHEN pg_is_in_recovery() OR n>10000 OR bytes>4194304
             THEN '{"unavailable":true}'::jsonb
             ELSE (SELECT coalesce(jsonb_agg(value),'[]'::jsonb) FROM records) END FROM bounds
    $cmd$, shard_name('canonical.events'::regclass,wanted),source_filter,source_filter,since_filter,since_filter);
    FOR placement IN
      SELECT n.nodename,n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n USING(groupid)
      WHERE p.shardid=wanted AND p.shardstate=1 AND n.noderole='primary' AND n.isactive
      ORDER BY n.nodeid
    LOOP
      -- With shard replication factor 2 there are two active copies. Read one
      -- copy; if that node is unavailable, fall through to the other copy.
      BEGIN
        SELECT * INTO STRICT response FROM master_run_on_worker(
          ARRAY[placement.nodename], ARRAY[placement.nodeport], ARRAY[command], false);
        IF response.success THEN
          payload := response.result::jsonb;
          IF jsonb_typeof(payload) = 'array' THEN
            RETURN payload;
          END IF;
        END IF;
      EXCEPTION WHEN OTHERS THEN
        -- Try the next active placement.
        NULL;
      END;
    END LOOP;
    RAISE EXCEPTION 'event shard unavailable';
END;
$$;
-- statement
REVOKE ALL ON FUNCTION canonical.read_shard(bigint,text,text) FROM PUBLIC;
-- statement
GRANT EXECUTE ON FUNCTION canonical.read_shard(bigint,text,text) TO canonical_app;
-- statement
