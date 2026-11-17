#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no Parquet support
# A subcolumn requested together with its column (`bbox`, `bbox.xmin`) or a dynamic subcolumn (`j.a`) is extracted
# from the column after reading. When another condition moved to PREWHERE, the read step lost such subcolumns and
# the remaining filter failed with `NOT_FOUND_COLUMN_IN_BLOCK`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -f "${DATA_FILE}".*' EXIT

${CLICKHOUSE_LOCAL} --query "
    SELECT tuple(number, number * 2)::Tuple(xmin UInt64, y UInt64) AS bbox, toString(number % 3) AS c, number AS id
    FROM numbers(20) INTO OUTFILE '${DATA_FILE}.tuple.parquet' TRUNCATE FORMAT Parquet;
    SELECT number AS id, if(number % 2, tuple(number, 'x'), NULL)::Nullable(Tuple(a UInt64, b String)) AS t
    FROM numbers(10) INTO OUTFILE '${DATA_FILE}.nullable_tuple.parquet' TRUNCATE FORMAT Parquet;
    SELECT number AS id, ('{\"a\":' || toString(number) || '}')::JSON AS j, toString(number % 2) AS c
    FROM numbers(10) INTO OUTFILE '${DATA_FILE}.json.parquet' TRUNCATE FORMAT Parquet;"

${CLICKHOUSE_LOCAL} --query "
    SELECT id, bbox FROM file('${DATA_FILE}.tuple.parquet') WHERE bbox.xmin > 5 AND c = '1' ORDER BY id;
    SELECT t.a FROM file('${DATA_FILE}.nullable_tuple.parquet') WHERE t IS NOT NULL ORDER BY t.a;
    SELECT id, j.a FROM file('${DATA_FILE}.json.parquet') WHERE j.a::Int64 > 3 AND c = '1' ORDER BY id;"
