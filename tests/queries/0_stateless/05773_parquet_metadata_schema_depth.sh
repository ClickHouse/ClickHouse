#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the `ParquetMetadata` format needs Arrow, which is not built in the fast test.

# The `ParquetMetadata` format limits the nesting depth of the Parquet schema with `max_parser_depth`,
# because Arrow builds the schema tree recursively.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

type='UInt8'
for _ in $(seq 1 100); do type="Tuple(a $type)"; done

file="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
$CLICKHOUSE_LOCAL --max_parser_depth 10000 -q "SELECT defaultValueOfTypeName('$type') AS c FORMAT Parquet" > "$file"

$CLICKHOUSE_LOCAL -q "SELECT num_columns FROM file('$file', ParquetMetadata)"
$CLICKHOUSE_LOCAL --max_parser_depth 50 -q "SELECT num_columns FROM file('$file', ParquetMetadata)" 2>&1 | grep -o -m1 'TOO_DEEP_RECURSION'

rm -f "$file"
