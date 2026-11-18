"""Build a private Patroni config from explicit Secret environment references."""
import json
import os
from pathlib import Path
import ssl
import time
import urllib.error
import urllib.request


def config():
    group = int(os.environ["CITUS_GROUP"])
    ip = os.environ["POD_IP"]
    return {
        "scope": f"bnpb-canonical-{group}", "name": os.environ["POD_NAME"],
        "citus": {"group": group, "database": "canonical"},
        "kubernetes": {
            "namespace": os.environ["POD_NAMESPACE"], "use_endpoints": True,
            "scope_label": "patroni-scope", "role_label": "postgres-role",
            "leader_label_value": "primary", "follower_label_value": "replica",
            "labels": {"app.kubernetes.io/part-of": "bnpb", "canonical-store": "true"},
            "pod_ip": ip, "ports": [{"name": "postgres", "port": 5432}],
        },
        "restapi": {"listen": "0.0.0.0:8008", "connect_address": f"{ip}:8008",
                    "authentication": {"username": "patroni", "password": os.environ["PATRONI_API_PASSWORD"]}},
        "bootstrap": {
            "dcs": {"ttl": 30, "loop_wait": 5, "retry_timeout": 5,
                    "primary_start_timeout": 10,
                    "synchronous_mode": True, "synchronous_mode_strict": True,
                    "synchronous_node_count": 1, "check_timeline": True,
                    "maximum_lag_on_failover": 0,
                    "postgresql": {"use_pg_rewind": True,
                                   "parameters": {"synchronous_commit": "remote_apply", "max_connections": 100,
                                                  "max_prepared_transactions": 200, "shared_buffers": "128MB",
                                                  "wal_level": "logical", "max_replication_slots": 32,
                                                  "max_wal_senders": 32,
                                                  "password_encryption": "scram-sha-256"}}},
            "initdb": [{"encoding": "UTF8"}],
            "post_bootstrap": "/opt/patroni/bin/python /opt/canonical/bootstrap.py",
        },
        "postgresql": {
            "listen": "0.0.0.0:5432", "connect_address": f"{ip}:5432",
            "data_dir": "/data/pgdata", "bin_dir": "/usr/lib/postgresql/16/bin",
            "pgpass": "/tmp/pgpass", "parameters": {"unix_socket_directories": "/tmp"},
            "authentication": {
                "superuser": {"username": "patroni", "password": os.environ["PATRONI_SUPERUSER_PASSWORD"]},
                "replication": {"username": "replicator", "password": os.environ["PATRONI_REPLICATION_PASSWORD"]}},
            "pg_hba": ["local all all trust", "host replication replicator all scram-sha-256",
                       "host replication patroni all scram-sha-256",
                       "host all patroni all scram-sha-256", "host all canonical_app all scram-sha-256"],
        },
        "watchdog": {"mode": "off"},
    }


def wait_initial_primary():
    if os.getenv("INITIAL_REPLICA") != "true" or Path("/data/pgdata/PG_VERSION").exists():
        return
    directory = Path("/var/run/secrets/kubernetes.io/serviceaccount")
    group = os.environ["CITUS_GROUP"]
    # Patroni names the DCS endpoint by scope and group, e.g.
    # bnpb-canonical-0-0-config. The scope is already group-specific.
    endpoint = ("https://" + os.environ["KUBERNETES_SERVICE_HOST"] + ":" + os.environ["KUBERNETES_SERVICE_PORT_HTTPS"]
                + "/api/v1/namespaces/" + os.environ["POD_NAMESPACE"] + "/endpoints/bnpb-canonical-"
                + group + "-" + group + "-config")
    context = ssl.create_default_context(cafile=str(directory / "ca.crt"))
    # The initial primary slot must finish bootstrap before an empty standby
    # competes for initialization. Existing DCS state permits later replica repair.
    while True:
        request = urllib.request.Request(endpoint, headers={"Authorization": "Bearer " + (directory / "token").read_text().strip()})
        try:
            with urllib.request.urlopen(request, context=context, timeout=5) as response:
                if json.load(response).get("metadata", {}).get("annotations", {}).get("initialize"):
                    return
        except urllib.error.HTTPError as exc:
            if exc.code not in (404, 429, 500, 503):
                raise
        except (urllib.error.URLError, TimeoutError):
            pass
        time.sleep(2)


if __name__ == "__main__":
    wait_initial_primary()
    path = Path("/tmp/patroni.json")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(config(), stream)
    os.execvp("patroni", ["patroni", str(path)])
