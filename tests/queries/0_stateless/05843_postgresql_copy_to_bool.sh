#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs psql and the `postgresql` table function, which is built only with libpqxx

# `COPY ... TO STDOUT` over the PostgreSQL protocol writes a `Bool` as `t`/`f`, the way PostgreSQL writes
# a `boolean` in both the text and the CSV format, and not as `true`/`false`. The `postgresql` table
# function pointed at this server reads its rows with such a `COPY` and accepts only that spelling.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The user name must be unique per test run, so that concurrent runs do not collide.
PG_USER="postgresql_user_05843_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} IDENTIFIED WITH plaintext_password BY 'pgpass';
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${PG_USER};
CREATE TABLE copy_bool (id UInt32, b Bool, n Nullable(Bool)) ENGINE = MergeTree ORDER BY id;
INSERT INTO copy_bool VALUES (1, true, false), (2, false, NULL);
"

PGPASSWORD=pgpass psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" \
    --no-psqlrc --no-align --tuples-only --quiet 2>&1 <<'EOF'
COPY (SELECT id, b, n FROM copy_bool ORDER BY id) TO STDOUT;
COPY (SELECT id, b, n FROM copy_bool ORDER BY id) TO STDOUT WITH (FORMAT csv);
EOF

${CLICKHOUSE_CLIENT} -q "
SELECT '--- reading the table back through the PostgreSQL protocol';
SELECT id, b, n FROM postgresql('localhost:${CLICKHOUSE_PORT_POSTGRESQL}', '${CLICKHOUSE_DATABASE}', 'copy_bool', '${PG_USER}', 'pgpass') ORDER BY id;
"

# A separate invocation, so that the user is dropped even if the read above fails.
${CLICKHOUSE_CLIENT} -q "
DROP TABLE copy_bool;
DROP USER ${PG_USER};
"
