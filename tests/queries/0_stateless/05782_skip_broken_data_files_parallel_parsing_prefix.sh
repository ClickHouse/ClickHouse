#!/usr/bin/env bash
# With parallel parsing, the format may read the input while it is created, past its first buffer:
# for `JSONEachRow`, to skip the whitespace before a possible `[`. A broken compressed stream there is
# an error of the input file, which `engine_file_skip_failed_data_files` skips, as with sequential parsing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=${CLICKHOUSE_USER_FILES_UNIQUE:?}

cleanup() {
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

mkdir -p "$DATA_DIR"

printf '{"a":1}\n' | gzip -c > "$DATA_DIR/good.json.gz"
# A valid `gzip` member of whitespace, followed by data that is not `gzip`.
printf '   ' | gzip -c > "$DATA_DIR/broken.json.gz"
printf 'this is not a gzip stream' >> "$DATA_DIR/broken.json.gz"

QUERY="SELECT sum(a) FROM file('$DATA_DIR/{good,broken}.json.gz', JSONEachRow, 'a UInt32')"

# The code of the error depends on the library that decompresses `gzip`: `libdeflate` or `zlib`.
$CLICKHOUSE_CLIENT -q "$QUERY SETTINGS max_threads = 1, max_parsing_threads = 4, input_format_parallel_parsing = 1" 2>&1 \
    | grep -q -E "CANNOT_DECOMPRESS|ZLIB_INFLATE_FAILED" && echo 'decompression error'

# Every skipped file is logged with a warning, so silence server logs.
for max_parsing_threads in 4 1; do
    $CLICKHOUSE_CLIENT --send_logs_level=fatal -q "
        $QUERY SETTINGS engine_file_skip_failed_data_files = 1, max_threads = 1,
            max_parsing_threads = $max_parsing_threads, input_format_parallel_parsing = 1"
done
