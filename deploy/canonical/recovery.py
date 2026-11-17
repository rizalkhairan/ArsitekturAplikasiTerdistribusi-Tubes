"""Administrative recovery operations, executed inside the coordinator pod.

Credentials never leave Secret-backed environment variables. No operation erases
data or forces promotion. Patroni owns elections, rewind and empty-disk cloning.
"""
from contextlib import contextmanager
import base64
import json
import os
import sys
import urllib.request

@contextmanager
def connect(host):
    import psycopg2
    connection = psycopg2.connect(host=host, port=5432, dbname="canonical", user="patroni",
        password=os.environ["PATRONI_SUPERUSER_PASSWORD"], connect_timeout=3,
        options="-c statement_timeout=10000 -c lock_timeout=3000")
    try:
        yield connection
    finally:
        connection.close()


def query(connection, sql, params=None):
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall() if cursor.description else []


def api(host, path, body=None):
    auth = base64.b64encode(("patroni:" + os.environ["PATRONI_API_PASSWORD"]).encode()).decode()
    request = urllib.request.Request(f"http://{host}:8008{path}",
        headers={"Authorization": "Basic " + auth, "Content-Type": "application/json"},
        data=json.dumps(body).encode() if body is not None else None,
        method="PATCH" if body is not None else "GET")
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.load(response)


def formation(connection, pods):
    nodes = query(connection, "SELECT groupid,nodename FROM pg_dist_node WHERE noderole='primary' AND isactive ORDER BY groupid")
    if [row[0] for row in nodes] != [0, 1, 2]:
        raise ValueError("Expected exactly Citus groups 0, 1 and 2")
    result = []
    for group, host in nodes:
        matches = [p for p in pods if p["ip"] == host and p["group"] == group]
        if len(matches) != 1:
            raise ValueError("Citus routing does not match a current group member")
        leader = matches[0]
        api(host, "/primary")  # Requires PostgreSQL primary and the DCS leader lock.
        config = api(host, "/config")
        if (config.get("synchronous_mode") is not True or config.get("check_timeline") is not True
                or config.get("maximum_lag_on_failover") != 0 or config.get("pause")):
            raise ValueError("Unsafe or paused Patroni election configuration")
        with connect(host) as database:
            if query(database, "SELECT pg_is_in_recovery()")[0][0]:
                raise ValueError("Citus routed to a standby")
            streaming = query(database, "SELECT application_name,sync_state FROM pg_stat_replication WHERE state='streaming'")
        result.append({"group": group, "host": host, "leader": leader,
                       "strict": config.get("synchronous_mode_strict") is True,
                       "streaming": streaming})
    return result


def verify_copies(groups, pods):
    """Bind replay evidence to specific pods, disks and distinct hosting nodes."""
    for group in groups:
        leader = group["leader"]
        pair = [p for p in pods if p["group"] == group["group"] and p["name"] != leader["name"]]
        if len(pair) != 1 or pair[0]["node"] == leader["node"]:
            raise ValueError("Each group needs a replica on a distinct hosting node")
        replica = pair[0]
        if [replica["name"], "sync"] not in [list(r) for r in group["streaming"]]:
            raise ValueError("Expected synchronous streaming replica is absent")
        api(replica["ip"], "/sync")
        with connect(group["host"]) as primary:
            system, lsn = query(primary, "SELECT system_identifier::text, pg_current_wal_flush_lsn()::text FROM pg_control_system()")[0]
        with connect(replica["ip"]) as standby:
            row = query(standby, "SELECT pg_is_in_recovery(), system_identifier::text, pg_last_wal_replay_lsn() >= %s::pg_lsn FROM pg_control_system()", (lsn,))[0]
            if row != (True, system, True):
                raise ValueError("Replica has not replayed the primary WAL barrier")


def set_policy(connection, durability, node=None):
    # UPDATE waits for the shared locks held by all application transactions.
    # The initial downgrade must commit even when the coordinator has no copy.
    query(connection, "SET LOCAL synchronous_commit = 'local'" if durability == "single_copy"
          else "SET LOCAL synchronous_commit = 'remote_apply'")
    query(connection, "UPDATE canonical.recovery SET durability=%s,fenced_node=%s,changed_at=clock_timestamp() WHERE singleton", (durability, node))
    connection.commit()


def run(request):
    pods, action = request["pods"], request["action"]
    if len(pods) != 6 or len({p["name"] for p in pods}) != 6:
        raise ValueError("Expected the six declared database slots")
    with connect("/tmp") as coordinator:
        coordinator.autocommit = True
        if not query(coordinator, "SELECT pg_try_advisory_lock(70408001)")[0][0]:
            raise ValueError("Another recovery or membership operation is running")
        # A session lock survives the policy COMMITs, serializing administrators.
        coordinator.autocommit = False
        groups = formation(coordinator, pods)
        mode, fenced_node = query(coordinator, "SELECT durability,fenced_node FROM canonical.recovery WHERE singleton")[0]
        if action == "status":
            return {"durability": mode, "fenced_node": fenced_node, "groups": groups,
                    "rebalance": query(coordinator, "SELECT row_to_json(s) FROM citus_rebalance_status() s")}
        if action == "degrade":
            node = request["node"]
            if mode == "single_copy" and fenced_node != node:
                raise ValueError("Repair the first lost node before another degradation")
            affected = [g for g in groups if any(p["node"] == node and p["group"] == g["group"] for p in pods)]
            if len(affected) != 2 or any(g["leader"]["node"] == node for g in groups):
                raise ValueError("Wait for safe Patroni promotion away from the fenced worker")
            set_policy(coordinator, "single_copy", node)
            for group in affected:
                api(group["host"], "/config", {"synchronous_mode_strict": False})
            return {"durability": "single_copy", "fenced_node": node}
        if action == "repair":
            verify_copies(groups, pods)
            # Strict first, then replicated acknowledgements. An interrupted
            # repair remains conservatively single_copy and can be retried.
            for group in groups:
                api(group["host"], "/config", {"synchronous_mode_strict": True})
            groups = formation(coordinator, pods)
            if not all(g["strict"] for g in groups):
                raise ValueError("Waiting for strict replication configuration")
            verify_copies(groups, pods)
            if not query(coordinator, "SELECT canonical.replication_ready()")[0][0]:
                raise ValueError("Waiting for all synchronous replicas")
            set_policy(coordinator, "replicated")
            return {"durability": "replicated", "copies_verified": 3}
        if mode != "replicated" or not all(g["strict"] for g in groups):
            raise ValueError("Membership/rebalance requires repaired strict replication")
        verify_copies(groups, pods)
        if action in ("plan", "rebalance", "drain", "enable"):
            if query(coordinator, "SELECT count(*) FROM citus_rebalance_status() WHERE state NOT IN ('finished','failed','cancelled')")[0][0]:
                raise ValueError("Wait for the current rebalance to finish")
            for group in groups:
                with connect(group["host"]) as database:
                    if query(database, "SHOW wal_level")[0][0] != "logical":
                        raise ValueError("Online rebalance requires wal_level=logical on all groups")
        if action == "check":
            if not query(coordinator, "SELECT canonical.replication_ready()")[0][0]:
                raise ValueError("Synchronous replication is not ready")
            return {"durability": mode, "copies_verified": 3}
        if action == "configure":
            for group in groups:
                api(group["host"], "/config", {"primary_start_timeout": 10,
                    "postgresql": {"parameters": {"wal_level": "logical", "max_replication_slots": 32,
                                                   "max_wal_senders": 32}}})
            return {"configuration": "updated", "note": "Check pending_restart; roll standbys before primaries if required"}
        if action in ("drain", "enable"):
            group = next(g for g in groups if g["group"] == request["group"])
            if group["group"] == 0:
                raise ValueError("Coordinator membership is fixed")
            query(coordinator, "SELECT citus_set_node_property(%s,5432,'shouldhaveshards',%s)",
                  (group["host"], action == "enable"))
            coordinator.commit()
            # Drain/enable changes capacity; PostgreSQL groups and their replicas
            # stay registered. Never remove a node still holding shard placements.
            return {"group": group["group"], "shouldhaveshards": action == "enable"}
        if action == "plan":
            return query(coordinator, "SELECT row_to_json(p) FROM get_rebalance_table_shards_plan(rebalance_strategy:='by_shard_count') p")
        if action == "rebalance":
            # The supported Citus API uses logical transfer in auto mode. All
            # canonical distributed tables have primary keys (replica identity).
            coordinator.commit()
            coordinator.autocommit = True
            query(coordinator, "SELECT citus_rebalance_start(rebalance_strategy:='by_shard_count', shard_transfer_mode:='auto')")
            return {"rebalance": "started"}
        raise ValueError("Unknown recovery action")


if __name__ == "__main__":
    try:
        print(json.dumps(run(json.load(sys.stdin)), default=str))
    except Exception as exc:
        # Connection exceptions may include supplied connection parameters.
        print("Recovery failed: " + (str(exc) if isinstance(exc, ValueError) else type(exc).__name__), file=sys.stderr)
        sys.exit(1)
