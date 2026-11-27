#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# The OIDs of the built-in functions and schemas are fixed by PostgreSQL (`pg_proc.dat`,
# `pg_namespace.dat`), and a PostgreSQL client may resolve them by OID alone. The emulated `pg_proc`
# had OIDs that PostgreSQL does not use for these functions, `pg_toast` had the OID 11519 instead
# of 99, and 99 was the OID of `pg_temp_1`. The type `name` had no `typelem`, while in PostgreSQL
# it is `char`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PG_USER="postgresql_user_05896_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${PG_USER};
"

function run_psql()
{
    psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" \
        --no-align --tuples-only --quiet -c "$1"
}

echo "--- pg_proc: the input, output and receive functions"
run_psql "SELECT oid, proname FROM pg_proc ORDER BY oid"

echo "--- pg_namespace: the schemas that have a fixed OID in PostgreSQL"
run_psql "SELECT oid, nspname FROM pg_namespace WHERE nspname IN ('pg_catalog', 'pg_toast') ORDER BY oid"

echo "--- pg_namespace: no two schemas have the same OID"
run_psql "SELECT count() - uniqExact(oid) FROM pg_namespace"

echo "--- pg_type: the element type of name"
run_psql "
    SELECT t.oid, t.typname, t.typcategory, t.typelem, e.typname
    FROM pg_type AS t
    JOIN pg_type AS e ON t.typelem = e.oid
    WHERE t.typname = 'name'"

${CLICKHOUSE_CLIENT} -q "DROP USER ${PG_USER};"
