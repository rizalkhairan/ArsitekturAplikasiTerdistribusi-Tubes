#!/usr/bin/env python3
"""Isolated Citus SQL/physical-replication acceptance test; no cluster mutations.

Three primaries and three standbys share the Docker kernel. This verifies SQL
transactions and replication, not Patroni promotion or separate-kernel durability.
Only containers/network created by this invocation are removed.
"""
import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import subprocess
import select
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
IMAGE = "citusdata/citus:13.0.1-pg16"


def run(*args, **kwargs):
    kwargs.setdefault("timeout", 120)
    result = subprocess.run(args, text=True, capture_output=True, **kwargs)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or f"{args[0]} failed ({result.returncode})")
    return result.stdout.strip()


def wait(check, description):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except (subprocess.CalledProcessError, RuntimeError):
            pass
        time.sleep(1)
    raise RuntimeError(description + " did not become ready")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recovery", action="store_true", help="also test SQL recovery, empty replicas and a second simulated hosting-node loss; no Patroni")
    args = parser.parse_args()
    name = "bnpb-a07-" + uuid.uuid4().hex[:10]
    containers = []
    network_created = False
    def sql(container, statement):
        return run("docker", "exec", container, "psql", "-h", "127.0.0.1", "-U", "postgres", "-d", "canonical", "-At", "-v", "ON_ERROR_STOP=1", "-c", statement)
    try:
        run("docker", "network", "create", name)
        network_created = True
        primaries = [name + f"-g{i}p" for i in range(3)]
        for i, primary in enumerate(primaries):
            port = ["-p", "127.0.0.1::5432"] if i == 0 else []
            run("docker", "run", "-d", "--name", primary, "--network", name,
                "--tmpfs", "/var/lib/postgresql/data:rw", *port,
                "-e", "POSTGRES_DB=canonical", "-e", "POSTGRES_HOST_AUTH_METHOD=trust", IMAGE,
                "postgres", "-c", "max_prepared_transactions=200", "-c", "synchronous_commit=remote_apply",
                "-c", "wal_level=logical", "-c", "max_replication_slots=32", "-c", "max_wal_senders=32")
            containers.append(primary)
        for primary in primaries:
            wait(lambda: sql(primary, "SELECT 1") == "1", primary)
            sql(primary, "CREATE EXTENSION IF NOT EXISTS citus; CREATE ROLE canonical_app LOGIN; ALTER ROLE canonical_app SET synchronous_commit='remote_apply';")
            run("docker", "exec", "-i", primary, "bash", "-c", 'cat >> "$PGDATA/pg_hba.conf"', input="\nhost replication postgres all trust\n")
            sql(primary, "SELECT pg_reload_conf()")
        sql(primaries[0], f"SELECT citus_set_coordinator_host('{primaries[0]}',5432); SELECT citus_add_node('{primaries[1]}',5432); SELECT citus_add_node('{primaries[2]}',5432);")
        # Seed schema before cloning; all later test writes use synchronous copies.
        schema = (ROOT / "services/bnpb/internal/canonical/schema.sql").read_text()
        run("docker", "exec", "-i", primaries[0], "psql", "-U", "postgres", "-d", "canonical", "-v", "ON_ERROR_STOP=1", "-1", "-f", "-", input=schema)
        for i, primary in enumerate(primaries):
            replica = name + f"-g{i}s"
            command = ('pg_basebackup -d "host=' + primary + ' user=postgres application_name=replica" '
                       '-D "$PGDATA" -R -X stream --checkpoint=fast; exec postgres -c max_prepared_transactions=200 -c synchronous_commit=remote_apply -c wal_level=logical -c max_replication_slots=32 -c max_wal_senders=32')
            run("docker", "run", "-d", "--name", replica, "--network", name, "--user", "postgres",
                "--tmpfs", "/replica:rw,uid=999,gid=999", "--tmpfs", "/var/lib/postgresql/data:rw", "-e", "PGDATA=/replica/pgdata",
                "--entrypoint", "bash", IMAGE, "-ec", command)
            containers.append(replica)
            wait(lambda: sql(replica, "SELECT pg_is_in_recovery()") == "t", replica)
            sql(primary, "ALTER SYSTEM SET synchronous_standby_names='FIRST 1 (replica)'")
            sql(primary, "SELECT pg_reload_conf()")
            wait(lambda: sql(primary, "SELECT count(*) FROM pg_stat_replication WHERE sync_state='sync'") == "1", primary + " sync standby")
            print(f"Group {i}: synchronous standby ready", flush=True)
        port = json.loads(run("docker", "inspect", primaries[0]))[0]["NetworkSettings"]["Ports"]["5432/tcp"][0]["HostPort"]
        env = {**os.environ, "CANONICAL_TEST_DSN": f"postgresql://postgres@127.0.0.1:{port}/canonical?sslmode=disable",
               "CANONICAL_TEST_REPLICATION": "1"}
        print("Running SQL/version/fencing/outbox/import tests with three synchronous pairs", flush=True)
        subprocess.run(["go", "test", "-race", "-count=1", "-v", "./internal/canonical"], cwd=ROOT / "services/bnpb", env=env, check=True)
        for i, primary in enumerate(primaries):
            lsn = sql(primary, "SELECT pg_current_wal_flush_lsn()")
            wait(lambda: sql(name + f"-g{i}s", f"SELECT pg_last_wal_replay_lsn() >= '{lsn}'::pg_lsn") == "t", "standby WAL replay")
        print("All three standbys replayed the tested commits", flush=True)
        print("Shard placements:", sql(primaries[0], "SELECT nodename,count(*) FROM citus_shards WHERE table_name='canonical.events'::regclass GROUP BY nodename ORDER BY nodename"), flush=True)
        if args.recovery:
            recovery(name, primaries, containers, sql, env)
        else:
            run("docker", "stop", name + "-g1s")
            env["CANONICAL_TEST_UNAVAILABLE"] = "1"
            subprocess.run(["go", "test", "-count=1", "-v", "./internal/canonical", "-run", "TestReplicationUnavailable"], cwd=ROOT / "services/bnpb", env=env, check=True)
        print("Citus SQL and synchronous-replication checks passed; Patroni/VM acceptance remains separate.", flush=True)
    except Exception:
        for container in containers:
            result = subprocess.run(["docker", "logs", "--tail", "12", container], capture_output=True, text=True)
            print(container + ":\n" + result.stdout + result.stderr)
        raise
    finally:
        if containers:
            subprocess.run(["docker", "rm", "-fv", *containers], check=True, stdout=subprocess.DEVNULL)
        if network_created:
            subprocess.run(["docker", "network", "rm", name], check=True, stdout=subprocess.DEVNULL)


def recovery(name, primaries, containers, sql, env):
    """SQL mechanism test. Manual promotion is deliberately NOT Patroni evidence.

    Hosting layout: control=(g0p,g2s), app=(g0s,g1p), store=(g1s,g2p).
    Lose app, rebuild both copies on empty disks, then lose store.
    """
    env = {**env, "CANONICAL_TEST_SCHEMA_READY": "1"}
    replicas = [name + f"-g{i}s" for i in range(3)]
    coordinator = primaries[0]

    def tests(durability):
        subprocess.run(["go", "test", "-count=1", "-v", "./internal/canonical", "-run", "^TestRecoveryDurability$"],
            cwd=ROOT / "services/bnpb", env={**env, "CANONICAL_TEST_RECOVERY": durability}, check=True)

    def checksum():
        # Includes tombstones, versions, outbox state and coordinator-local state.
        return sql(coordinator, "SELECT md5(string_agg(value,E'\\n' ORDER BY value COLLATE \"C\")) FROM ("
            "SELECT row_to_json(e)::text AS value FROM canonical.events e UNION ALL "
            "SELECT row_to_json(o)::text FROM canonical.outbox o UNION ALL "
            "SELECT row_to_json(a)::text FROM canonical.authorities a UNION ALL "
            "SELECT row_to_json(w)::text FROM canonical.windows w) all_rows")

    def snapshot():
        return set(sql(coordinator, "SELECT row_to_json(e)::text FROM canonical.events e UNION ALL "
            "SELECT row_to_json(o)::text FROM canonical.outbox o UNION ALL "
            "SELECT row_to_json(a)::text FROM canonical.authorities a UNION ALL "
            "SELECT row_to_json(w)::text FROM canonical.windows w").splitlines())

    @contextmanager
    def traffic(durability):
        with tempfile.TemporaryDirectory(prefix="bnpb-a08-traffic-") as directory:
            stop = Path(directory, "stop")
            workload = subprocess.Popen(["go", "test", "-count=1", "-v", "./internal/canonical", "-run", "^TestOnlineRebalanceWorkload$"],
                cwd=ROOT / "services/bnpb", env={**env, "CANONICAL_TEST_REBALANCE": "1",
                "CANONICAL_TEST_WORKLOAD_STOP": str(stop), "CANONICAL_TEST_ONLINE_DURABILITY": durability}, stdout=subprocess.PIPE, text=True)
            try:
                deadline, ready = time.monotonic() + 30, False
                while time.monotonic() < deadline and workload.poll() is None:
                    if select.select([workload.stdout], [], [], 1)[0]:
                        line = workload.stdout.readline()
                        print(line, end="", flush=True)
                        if "A08_WORKLOAD_READY" in line:
                            ready = True
                            break
                if not ready:
                    raise RuntimeError("Canonical workload did not start before the online operation")
                yield
                stop.touch()
                output, _ = workload.communicate(timeout=30)
                print(output, end="", flush=True)
                if workload.returncode != 0:
                    raise RuntimeError("Canonical reads/writes failed during the online operation")
            finally:
                if workload.poll() is None:
                    workload.terminate()
                    workload.wait(timeout=10)
                workload.stdout.close()

    def sync(group, enabled):
        value = "FIRST 1 (replica)" if enabled else ""
        sql(primaries[group], f"ALTER SYSTEM SET synchronous_standby_names='{value}'")
        sql(primaries[group], "SELECT pg_reload_conf()")

    def route(group):
        sql(coordinator, "SET synchronous_commit='local'; SELECT citus_update_node("
            f"(SELECT nodeid FROM pg_dist_node WHERE groupid={group} AND noderole='primary'),'{primaries[group]}',5432)")

    def promote(group):
        old_primary, survivor = primaries[group], replicas[group]
        run("docker", "exec", "--user", "postgres", survivor, "pg_ctl", "-D", "/replica/pgdata", "-w", "promote")
        primaries[group], replicas[group] = survivor, old_primary
        sync(group, False)

    def policy(value):
        sql(coordinator, "SET synchronous_commit='local'; UPDATE canonical.recovery SET durability='" + value + "' WHERE singleton")

    def empty_replica(group):
        replica, primary = replicas[group], primaries[group]
        run("docker", "rm", "-fv", replica)
        containers.remove(replica)
        command = ('pg_basebackup -d "host=' + primary + ' user=postgres application_name=replica" '
                   '-D "$PGDATA" -R -X stream --checkpoint=fast; exec postgres -c max_prepared_transactions=200 -c synchronous_commit=remote_apply -c wal_level=logical -c max_replication_slots=32 -c max_wal_senders=32')
        run("docker", "run", "-d", "--name", replica, "--network", name, "--user", "postgres",
            "--tmpfs", "/replica:rw,uid=999,gid=999", "--tmpfs", "/var/lib/postgresql/data:rw", "-e", "PGDATA=/replica/pgdata",
            "--entrypoint", "bash", IMAGE, "-ec", command)
        containers.append(replica)
        wait(lambda: sql(replica, "SELECT pg_is_in_recovery()") == "t", "empty replacement clone")
        sync(group, True)
        wait(lambda: sql(primary, "SELECT count(*) FROM pg_stat_replication WHERE sync_state='sync'") == "1", "replacement synchronous replication")
        lsn = sql(primary, "SELECT pg_current_wal_flush_lsn()")
        wait(lambda: sql(replica, f"SELECT pg_last_wal_replay_lsn() >= '{lsn}'::pg_lsn") == "t", "replacement replay barrier")

    # Exercise online redistribution with continuous canonical reads/writes.
    # Retain all three physical groups; drain/re-enable changes shard capacity.
    initial_rows = snapshot()
    sql(coordinator, "SELECT citus_set_node_property('" + primaries[2] + "',5432,'shouldhaveshards',false)")
    with traffic("replicated"):
        sql(coordinator, "SELECT citus_rebalance_start(rebalance_strategy:='by_shard_count')")
        wait(lambda: sql(coordinator, "SELECT count(*) FROM citus_shards WHERE table_name='canonical.events'::regclass AND nodename='" + primaries[2] + "'") == "0", "online drain")
        sql(coordinator, "SELECT citus_rebalance_wait()")
    sql(coordinator, "SELECT citus_set_node_property('" + primaries[2] + "',5432,'shouldhaveshards',true)")
    sql(coordinator, "SELECT citus_rebalance_start(rebalance_strategy:='by_shard_count')")
    sql(coordinator, "SELECT citus_rebalance_wait()")
    wait(lambda: sql(coordinator, "SELECT count(*) FROM (SELECT nodename,count(*) FROM citus_shards WHERE table_name='canonical.events'::regclass GROUP BY nodename HAVING count(*)=16) balanced") == "3", "balanced event shards")
    if not initial_rows.issubset(snapshot()):
        raise RuntimeError("Online rebalancing lost existing canonical data")
    tests("replicated")
    before = checksum()

    print("SQL scenario: lose app hosting slots (coordinator replica + group 1 primary)", flush=True)
    run("docker", "stop", replicas[0], primaries[1])
    policy("single_copy")
    sync(0, False)
    promote(1)
    route(1)
    if checksum() != before:
        raise RuntimeError("Data changed after first hosting-node loss")
    tests("single_copy")
    before_rows = snapshot()
    with traffic("single_copy"):
        for group in (0, 1):
            empty_replica(group)
    if sql(coordinator, "SELECT canonical.replication_ready()") != "t":
        raise RuntimeError("Redundancy was not restored")
    sql(coordinator, "UPDATE canonical.recovery SET durability='replicated' WHERE singleton")
    if not before_rows.issubset(snapshot()):
        raise RuntimeError("Empty-disk repair changed data")
    tests("replicated")
    before = checksum()

    print("SQL scenario: lose store hosting slots AFTER empty-disk repair (group 1 + group 2 primaries)", flush=True)
    run("docker", "stop", primaries[1], primaries[2])
    policy("single_copy")
    for group in (1, 2):
        promote(group)
    for group in (1, 2):
        route(group)
    if checksum() != before:
        raise RuntimeError("Repaired dataset did not survive the second hosting-node loss")
    tests("single_copy")
    print("SQL recovery preserved the complete dataset through a second loss; manual promotion/shared kernel, NOT live Patroni/demo acceptance", flush=True)


if __name__ == "__main__":
    main()
