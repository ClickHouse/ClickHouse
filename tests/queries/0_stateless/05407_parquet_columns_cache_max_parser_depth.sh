#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: Parquet support is not built in fast-test images
# no-parallel: the columns cache is system-wide, and the test drops it to reset its admission control

# `max_parser_depth` limits the nesting of the variant values decoded from a `Parquet` file: a column
# cached by a read with no limit must not be served to a read with a lower one, which has to raise
# the same exception as without the cache.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
FILE_PATH="${USER_FILES_PATH}/${FILE}"

# An unshredded variant (a group of the `metadata` and `value` blobs) holding 200 nested arrays.
VALUE=$(python3 -c "
import struct
v = b'\x00'
for _ in range(200):
    v = bytes([0x0F, 1]) + struct.pack('<II', 0, len(v)) + v
print(v.hex())")
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet)
    SELECT CAST(tuple(unhex('010000'), unhex('${VALUE}')), 'Tuple(metadata String, value String)') AS v
    SETTINGS output_format_parquet_string_as_string = 0, engine_file_truncate_on_insert = 1"
# Only a file that has not been modified for a few seconds is cached.
touch -d "1 minute ago" "${FILE_PATH}"

QUERY="SELECT length(toString(v)) FROM file('${FILE}', Parquet, 'v Dynamic')"
CACHE="use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1,
    columns_cache_max_bytes_to_write_to_cache = 0, columns_cache_max_estimated_bytes_to_write_to_cache = 0"

# Other tests may have lowered the admission probability of the cache; dropping the cache resets it.
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"

# The admission control of the cache can skip a write: read the file until the column is there.
for _ in {1..20}; do
    $CLICKHOUSE_CLIENT -q "${QUERY} FORMAT Null SETTINGS ${CACHE}, max_parser_depth = 0"
    [ "$($CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM system.columns_cache WHERE database = '' AND endsWith(part, '/${FILE}')")" = 1 ] && break
done

echo "--- no limit, from the cache"
$CLICKHOUSE_CLIENT -q "${QUERY} SETTINGS ${CACHE}, max_parser_depth = 0"
echo "--- low limit, with the cache"
$CLICKHOUSE_CLIENT -q "${QUERY} SETTINGS ${CACHE}, max_parser_depth = 100" 2>&1 | grep -o -m1 'TOO_DEEP_RECURSION'
echo "--- low limit, without the cache"
$CLICKHOUSE_CLIENT -q "${QUERY} SETTINGS use_columns_cache = 0, max_parser_depth = 100" 2>&1 | grep -o -m1 'TOO_DEEP_RECURSION'

rm -f "${FILE_PATH}"
