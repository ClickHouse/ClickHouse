#!/usr/bin/env bash
# Tags: no-fasttest
# ^ pyarrow is not available in the fast test

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# One large block split by a small `output_format_arrow_max_record_batch_size_bytes` into thousands of
# record batches. The `LowCardinality` column never needs a split itself, but measuring it is linear in
# the rows given, so this is the shape where measuring the whole remainder of the block
# for every batch would be quadratic. Every batch is bounded by the `String` column: 64 rows of 64 bytes.

FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_many_batches.arrows"
trap 'rm -f "$FILE"' EXIT

$CLICKHOUSE_LOCAL --query "
    SELECT
        toLowCardinality(toString(number % 1000)) AS lc,
        repeat('x', 64) AS s
    FROM numbers(262144)
    SETTINGS
        max_block_size = 262144,
        output_format_arrow_max_record_batch_size_bytes = 4096,
        output_format_arrow_compression_method = 'none'
    FORMAT ArrowStream" > "$FILE"

python3 - "$FILE" <<'PY'
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

with pa.OSFile(sys.argv[1], "rb") as src:
    batches = list(ipc.open_stream(src))
print("batches:", len(batches), "row counts:", sorted({batch.num_rows for batch in batches}))
PY

$CLICKHOUSE_LOCAL --query "
    SELECT count(), uniqExact(lc), sum(length(s))
    FROM file('$FILE', ArrowStream)"
