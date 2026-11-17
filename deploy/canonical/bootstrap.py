"""Provision the restricted application role on each independent Citus group."""
import os
import sys
import psycopg2

with psycopg2.connect(sys.argv[1]) as connection:
    with connection.cursor() as cursor:
        # Initial cluster formation precedes application traffic/replica cloning.
        cursor.execute("SET LOCAL synchronous_commit = 'local'")
        cursor.execute("CREATE ROLE canonical_app LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD %s",
                       (os.environ["CANONICAL_APP_PASSWORD"],))
        cursor.execute("ALTER ROLE canonical_app SET synchronous_commit = 'remote_apply'")
        cursor.execute("ALTER ROLE canonical_app SET statement_timeout = '8s'")
        cursor.execute("ALTER ROLE canonical_app SET lock_timeout = '3s'")
