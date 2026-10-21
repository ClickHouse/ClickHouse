#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no Parquet support
# `.pq` files are Parquet: `INTO OUTFILE` and `file` without a format write Parquet, and reading infers it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -f "${DATA_FILE}".*' EXIT

${CLICKHOUSE_LOCAL} --query "SELECT 1 AS a INTO OUTFILE '${DATA_FILE}.outfile.pq'"
${CLICKHOUSE_LOCAL} --query "SELECT a FROM file('${DATA_FILE}.outfile.pq', Parquet)"

${CLICKHOUSE_LOCAL} --query "INSERT INTO FUNCTION file('${DATA_FILE}.function.pq') SELECT 2 AS a"
${CLICKHOUSE_LOCAL} --query "SELECT a FROM file('${DATA_FILE}.function.pq', Parquet)"
${CLICKHOUSE_LOCAL} --query "SELECT a, _file LIKE '%.pq' FROM file('${DATA_FILE}.function.pq')"
