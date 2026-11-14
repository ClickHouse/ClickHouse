#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A server marshalls the result blocks of a secondary query with an intermediate stage into `ColumnBLOB`
# when the connection is compressed. clickhouse-client can send such a query, and it used to pass the blobs
# to a row output format: LOGICAL_ERROR 'ColumnBLOB should be converted to a regular column before usage'.

# Without `allow_special_serialization_kinds_in_output_formats` the client makes every column a full one anyway.
QUERY="SELECT number, toString(number) FROM numbers(1000)
    SETTINGS enable_parallel_blocks_marshalling = 1, allow_special_serialization_kinds_in_output_formats = 1"

for format in TSV CSV JSONEachRow
do
    echo -n "$format: "
    $CLICKHOUSE_CLIENT --compression 1 --query_kind secondary_query --stage with_mergeable_state \
        --query "$QUERY FORMAT $format" 2>&1 | wc -l
done
