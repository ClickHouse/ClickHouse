#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `--format` and `--output-format` options are only the default output format: the `FORMAT` clause of the query
# and the extension of the `INTO OUTFILE` file win over them. A `format` or `output_format` setting from `SET` or from
# the `SETTINGS` clause of the query still wins over the `FORMAT` clause.

out="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_05407.json"

echo "-- local, FORMAT clause"
${CLICKHOUSE_LOCAL} --format CSV --query "SELECT 'a' AS x FORMAT JSONEachRow"

echo "-- local, INTO OUTFILE with a FORMAT clause"
rm -f "$out"
${CLICKHOUSE_LOCAL} --format CSV --query "SELECT 'a' AS x INTO OUTFILE '$out' FORMAT JSONEachRow"
cat "$out"

echo "-- local, INTO OUTFILE without a FORMAT clause"
rm -f "$out"
${CLICKHOUSE_LOCAL} --output-format CSV --query "SELECT 'a' AS x INTO OUTFILE '$out'"
cat "$out"

echo "-- local, no FORMAT clause"
${CLICKHOUSE_LOCAL} --format CSV --query "SELECT 'a' AS x"

echo "-- local, SETTINGS clause"
${CLICKHOUSE_LOCAL} --format CSV --query "SELECT 'a' AS x FORMAT TSV SETTINGS output_format = 'JSONEachRow'"

echo "-- local, SET"
${CLICKHOUSE_LOCAL} --format CSV --query "SET output_format = 'JSONEachRow'; SELECT 'a' AS x FORMAT TSV"

echo "-- client, FORMAT clause"
${CLICKHOUSE_CLIENT} --format CSV --query "SELECT 'a' AS x FORMAT JSONEachRow"

echo "-- client, INTO OUTFILE with a FORMAT clause"
rm -f "$out"
${CLICKHOUSE_CLIENT} --output-format CSV --query "SELECT 'a' AS x INTO OUTFILE '$out' FORMAT JSONEachRow"
cat "$out"

echo "-- client, no FORMAT clause"
${CLICKHOUSE_CLIENT} --format CSV --query "SELECT 'a' AS x"

rm -f "$out"
