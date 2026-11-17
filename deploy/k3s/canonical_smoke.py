#!/usr/bin/env python3
"""Isolated Citus SQL/physical-replication acceptance test; no cluster mutations.

Three primaries and three standbys share the Docker kernel. This verifies SQL
transactions and replication, not Patroni promotion or separate-kernel durability.
Only containers/network created by this invocation are removed.
"""
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
IMAGE = "citusdata/citus:13.0.1-pg16"


def run(*args, **kwargs):
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
                "postgres", "-c", "max_prepared_transactions=200", "-c", "synchronous_commit=remote_apply")
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
                       '-D "$PGDATA" -R -X stream --checkpoint=fast; exec postgres -c max_prepared_transactions=200 -c synchronous_commit=remote_apply')
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


if __name__ == "__main__":
    main()
