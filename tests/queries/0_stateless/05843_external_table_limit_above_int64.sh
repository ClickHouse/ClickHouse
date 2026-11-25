#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs the `sqlite` table function and the `sqlite3` tool

# A `LIMIT` of a query is pushed down to an external database, where it is safe. A `LIMIT` in ClickHouse
# is any `UInt64`, while SQLite (and PostgreSQL) accept only a signed 64-bit value, so a `LIMIT` above
# that range - such as `LIMIT 18446744073709551615`, MySQL's idiom for "all rows" - made the query fail
# with `datatype mismatch`. Such a limit does not cut off anything and is not sent.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_limit_above_int64.db"
rm -f "${DB_PATH}"

sqlite3 "${DB_PATH}" 'CREATE TABLE t (id INTEGER); INSERT INTO t VALUES (1), (2), (3);'
chmod ugo+r "${DB_PATH}"

SOURCE="sqlite('${DB_PATH}', 't')"

${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM (SELECT id FROM ${SOURCE} LIMIT 9223372036854775808);
SELECT count() FROM (SELECT id FROM ${SOURCE} LIMIT 18446744073709551615);
SELECT count() FROM (SELECT id FROM ${SOURCE} LIMIT 9223372036854775807 OFFSET 1);
SELECT count() FROM (SELECT id FROM ${SOURCE} WHERE id > 1 LIMIT 18446744073709551615);

-- The largest limit that SQLite accepts, and an ordinary one.
SELECT count() FROM (SELECT id FROM ${SOURCE} LIMIT 9223372036854775807);
SELECT count() FROM (SELECT id FROM ${SOURCE} LIMIT 2);
"

rm -f "${DB_PATH}"
