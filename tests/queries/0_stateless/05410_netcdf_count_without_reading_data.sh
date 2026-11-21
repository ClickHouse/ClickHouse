#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_count.nc

# 16 MB of data.
$CLICKHOUSE_LOCAL -q "SELECT number::Int32 AS x FROM numbers(4000000) INTO OUTFILE '$FILE' FORMAT NetCDF"

# The number of rows is in the header, so a count with an explicit structure, which does not infer
# the schema, and without the cache of the counts reads the header and not the data.
$CLICKHOUSE_LOCAL --print-profile-events --profile-events-delay-ms=-1 -q "
    SELECT count() FROM file('$FILE', NetCDF, 'x Int32')
    SETTINGS use_cache_for_count_from_files = 0, optimize_count_from_files = 1, storage_file_read_method = 'pread'
" 2>&1 | grep -E '^[0-9]+$|ReadBufferFromFileDescriptorReadBytes' | sed -E 's/.*ReadBufferFromFileDescriptorReadBytes: ([0-9]+).*/\1/' | awk 'NR == 1 { print } NR == 2 { print ($1 < 4000000 ? "data is not read" : "data is read: " $1) }'

rm -f "$FILE"
