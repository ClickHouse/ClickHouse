#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# The emulated `pg_attribute` catalog advertises `UUID2` columns as `uuid` / `uuid[]`, like `UUID` and like
# `RowDescription` does, so that a self-connected `postgresql(...)` read infers the historical `UUID` type
# instead of `String`.
# See https://github.com/ClickHouse/ClickHouse/pull/110084

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The user name must be unique per test run: the flaky check runs this test many times concurrently.
PG_USER="postgresql_user_05403_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${PG_USER};

CREATE TABLE t (u UUID, u2 UUID2, n Nullable(UUID2), a Array(UUID2), an Array(Nullable(UUID2))) ENGINE = Memory;
INSERT INTO t VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0', '61f0c404-5cb3-11e7-907b-a6006ad3dba0', NULL, ['61f0c404-5cb3-11e7-907b-a6006ad3dba0'], [NULL]);
"

echo "--- catalog OIDs"
psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" \
    --no-psqlrc --tuples-only --no-align -c "
    SELECT a.attname, a.atttypid
    FROM pg_class AS c
    JOIN pg_attribute AS a ON a.attrelid = c.oid
    WHERE c.relname = 't'
    ORDER BY a.attnum" 2>&1

echo "--- self-connected schema inference"
${CLICKHOUSE_CLIENT} -q "
DESCRIBE TABLE postgresql('127.0.0.1:${CLICKHOUSE_PORT_POSTGRESQL}', '${CLICKHOUSE_DATABASE}', 't', '${PG_USER}', '')
FORMAT TSV;
" | cut -f 1,2

${CLICKHOUSE_CLIENT} -q "
DROP USER ${PG_USER};
"
