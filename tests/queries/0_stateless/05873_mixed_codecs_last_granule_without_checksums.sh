#!/usr/bin/env bash

# With adaptive codec selection the blocks of one data file are compressed with different codecs.
# A part without a final mark that lost its `checksums.txt` has its last granule checked by reading the data file. 
# That read used to fail on the first block of another codec, so an intact part was detached as broken.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" -q "
    CREATE TABLE t (v UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000, index_granularity_bytes = 0,
        max_compress_block_size = 1024, compress_marks = 0,
        enable_adaptive_codec_selection = 1, adaptive_codec_selection_period = 1;
    INSERT INTO t SELECT if(number % 1000 < 500, cityHash64(number), 0) FROM numbers(5000);
    OPTIMIZE TABLE t FINAL;
    SELECT 'several codecs', length(codec_block_counts) > 1 FROM mergeTreeCodecBlockCounts(currentDatabase(), t);
"

PART_PATH=$(${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" -q "SELECT path FROM system.parts WHERE table = 't' AND active")

${CLICKHOUSE_BINARY} check-marks "${PART_PATH}"/*.mrk "${PART_PATH}"/*.bin > /dev/null && echo "check-marks OK"
rm "${PART_PATH}/checksums.txt"

${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" -q "
    SELECT 'rows', count() FROM t;
    SELECT 'detached parts', count() FROM system.detached_parts;
"

rm -rf "${WORKING_DIR}"
