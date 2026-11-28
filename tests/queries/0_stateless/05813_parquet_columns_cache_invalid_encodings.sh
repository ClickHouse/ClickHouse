#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet support is not built in fast-test images

# `ColumnMetaData.encodings` of a `Parquet` file may hold values that are not valid encodings. The columns cache looks
# at the list to bound the size of a chunk; an unknown value must not be loaded as an enum (undefined behavior), and the
# file is read as without the cache.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
FILE_PATH="${USER_FILES_PATH}/${FILE}"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}.tmp', Parquet, 's String') SELECT 'abcdefghijklmnop' FROM numbers(1024)
    SETTINGS engine_file_truncate_on_insert = 1"

# Replace the encodings of the column chunk with `[100]`: `FileMetaData.row_groups` -> `RowGroup.columns` ->
# `ColumnChunk.meta_data` -> `ColumnMetaData.encodings`.
python3 - "${CUR_DIR}/helpers" "${FILE_PATH}.tmp" "${FILE_PATH}" <<'PYEOF'
import struct
import sys

sys.path.insert(0, sys.argv[1])
from parquet_strip_size_statistics import copy_struct

with open(sys.argv[2], "rb") as f:
    data = f.read()
footer_size = struct.unpack("<I", data[-8:-4])[0]
footer_start = len(data) - 8 - footer_size
footer = bytearray()
copy_struct(data[footer_start:-8], 0, {4: {1: {3: {2: (100,)}}}}, footer)
with open(sys.argv[3], "wb") as f:
    f.write(data[:footer_start] + bytes(footer) + struct.pack("<I", len(footer)) + b"PAR1")
PYEOF
rm -f "${FILE_PATH}.tmp"
# Only a file that has not been modified for a few seconds is cached.
touch -d "1 minute ago" "${FILE_PATH}"

SETTINGS="max_threads = 1, input_format_parquet_use_native_reader_v3 = 1, input_format_parquet_max_block_size = 1000"
QUERY="SELECT count(), sum(length(s)) FROM file('${FILE}', Parquet, 's String') SETTINGS ${SETTINGS}"

$CLICKHOUSE_CLIENT -q "
    ${QUERY}, use_columns_cache = 0;
    ${QUERY}, use_columns_cache = 1;
    ${QUERY}, use_columns_cache = 1;"

rm -f "${FILE_PATH}"
