#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no Parquet support
# Writing a constant `Geometry` (or other geo type) column to Parquet used to crash in `prepareGeoColumn`, which cast
# the column to `ColumnVariant` before it was materialized.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -f "${DATA_FILE}".*' EXIT

# Each case is a query writing a constant column and the query reading it back. `Geometry` is written as WKB.
while IFS='|' read -r WRITE READ
do
    echo "--- ${WRITE}"
    ${CLICKHOUSE_LOCAL} --query "${WRITE} INTO OUTFILE '${DATA_FILE}.parquet' TRUNCATE FORMAT Parquet" < /dev/null
    ${CLICKHOUSE_LOCAL} --query "DESC file('${DATA_FILE}.parquet'); ${READ} FROM file('${DATA_FILE}.parquet')" < /dev/null
done <<'CASES'
SELECT readWKT('POLYGON((0 0,1 0,1 1,0 0))') AS g|SELECT hex(g)
SELECT [[(1.,2.),(3.,4.),(1.,2.)]]::Polygon::Geometry AS g FROM numbers(2)|SELECT hex(g)
SELECT (1.5, 2.5)::Point AS p FROM numbers(2)|SELECT p
CASES
