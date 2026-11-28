#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# `PQsetClientEncoding` of libpq sends `set client_encoding to '...'`, and some clients call it while
# connecting, usually with `UNICODE`. The server sends text as UTF8 and has no such setting, so the
# statement failed with a syntax error and the client could not connect. It is now acknowledged as a
# no-op when the encoding is the one the server uses, like `client_encoding` of the startup packet.
# See https://github.com/ClickHouse/ClickHouse/issues/66318

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PG_USER="postgresql_user_05872_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
"

function run_psql()
{
    psql --host localhost --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" --no-align "$@" 2>&1
}

echo "--- the encoding command of psql calls PQsetClientEncoding"
run_psql --tuples-only -c '\encoding UNICODE' -c '\encoding' -c 'SELECT 1'

echo "--- the spellings of UTF8, and DEFAULT, which restores it"
run_psql <<'EOF'
set client_encoding to 'UNICODE';
SET client_encoding TO 'UTF8';
SET client_encoding = 'utf-8';
SET client_encoding = UTF8;
SET SESSION client_encoding TO 'Utf_8';
SET client_encoding TO DEFAULT;
SELECT 1 AS connection_is_still_usable;
EOF

# Only the last line is kept: psql 15 and newer prints the result of every statement of `-c`, so
# there is a `SET` line before the number, and an older psql prints only the last result.
echo "--- a statement after it in the same packet is executed"
run_psql --tuples-only -c "SET client_encoding TO 'UNICODE'; SELECT 20261124" | tail -n 1

# Another encoding would make the client decode the UTF8 text it gets as something else, and a
# malformed statement is not a no-op either: both fall through and fail.
echo "--- a statement with another encoding, or a malformed one, is not a no-op"
for rejected in "SET client_encoding TO 'LATIN1'" "SET client_encoding = 'SQL_ASCII'" "SET client_encoding TO 'DEFAULT'" \
                "SET client_encoding TO ''" "SET client_encoding TO 'UTF8' extra" "SET client_encoding"; do
    if run_psql -c "${rejected}" | grep -qi error; then
        echo "rejected: ${rejected}"
    else
        echo "UNEXPECTED: silently accepted: ${rejected}"
    fi
done

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${PG_USER};"
