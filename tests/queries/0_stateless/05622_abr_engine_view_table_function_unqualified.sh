#!/usr/bin/env bash
# Tags: shard

# With `serialize_query_plan = 1`, a remote server that reads an `ABR` table is sent the SQL of the query instead of a plan,
# which does not carry the filter that `ABR` chooses its table from. An `ABR` table read through the `view` table function
# with an unqualified name must be found in the current database, as the `view` table function finds it, and not in the
# pseudo-database of table functions where its view is created.
#
# `remote` runs the query on the shard in the default database of its connection, so the unqualified name is only found
# there if the tables are in `default`. Their names carry the test database to stay unique.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

prefix="src_${CLICKHOUSE_DATABASE}"
abr="abr_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS default.${abr}; DROP TABLE IF EXISTS default.${prefix}_1"
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --allow_experimental_abr 1 -q "
CREATE TABLE default.${prefix}_1 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
INSERT INTO default.${prefix}_1 SELECT toDateTime('2026-06-01 00:00:00') + number, 1 FROM numbers(100);
CREATE TABLE default.${abr} ENGINE = ABR('${prefix}', [1], ('ts'));
"

${CLICKHOUSE_CLIENT} -q "
USE default;
SELECT count(), max(value) FROM remote('127.0.0.2', view(SELECT * FROM ${abr}))
WHERE ts >= toDateTime('2026-06-01 00:00:00')
SETTINGS enable_analyzer = 1, serialize_query_plan = 1, prefer_localhost_replica = 0, analyzer_inline_views = 0;
"
