#!/usr/bin/env bash

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Older writers did not round the compressed block size to whole values, so a value could be split
# between two compressed blocks. `RECOMPRESS COLUMN` re-emits the blocks of a stream without
# deserializing the values, but a codec for a specific type (`Gorilla`, `Delta`, `ALP`, ...) only
# accepts whole values, so the split values have to be moved to the next block, and the marks
# remapped.

workdir=$(mktemp -d "${CLICKHOUSE_TMP}/recompress_unaligned.XXXXXX")
trap 'rm -rf "$workdir"' EXIT

part_dir=$($CLICKHOUSE_LOCAL --path "$workdir" --multiquery --query "
CREATE DATABASE d ENGINE = Atomic;
CREATE TABLE d.t (u UInt32 CODEC(LZ4), v Float64 CODEC(LZ4))
ENGINE = MergeTree ORDER BY u
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 8192;
SELECT concat(data_paths[1], 'detached') FROM system.tables WHERE database = 'd' AND name = 't';
")

# A part written by ClickHouse 25.8.1.5101 with `min_compress_block_size = max_compress_block_size = 65537`:
#     INSERT INTO d.t SELECT intDiv(number, 100), intDiv(number, 1000) / 4 FROM numbers(20000)
# The blocks of both `u.bin` and `v.bin` hold 65537 bytes, so they end in the middle of a value, and
# the marks of the granules point into the middle of the blocks.
base64 -d <<'PART' | tar -xz -C "$part_dir"
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
PART

$CLICKHOUSE_LOCAL --path "$workdir" --multiquery <<'SQL'
ALTER TABLE d.t ATTACH PART 'all_1_1_0';
SELECT 'before', count(), sum(u), sum(v) FROM d.t;
ALTER TABLE d.t MODIFY COLUMN u UInt32 CODEC(Delta, LZ4), MODIFY COLUMN v Float64 CODEC(Gorilla, LZ4);
ALTER TABLE d.t RECOMPRESS COLUMN u, RECOMPRESS COLUMN v SETTINGS mutations_sync = 2;
SELECT 'mutations', count(), countIf(is_done), max(latest_fail_reason) FROM system.mutations WHERE database = 'd' AND table = 't';
-- The streams were recompressed block by block, not rewritten from deserialized values.
SELECT 'raw blocks', value > 0 FROM system.events WHERE event = 'MutationRecompressedBlocks';
SELECT 'after', count(), sum(u), sum(v) FROM d.t;
SELECT 'mismatches', countIf(v != intDiv(u, 10) / 4) FROM d.t;
-- Reads starting at the marks of the second and the third granules.
SELECT 'granule 2', count(), sum(v) FROM d.t WHERE u BETWEEN 82 AND 163;
SELECT 'granule 3', count(), sum(v) FROM d.t WHERE u >= 164;
SELECT 'point', u, v FROM d.t WHERE u = 150 LIMIT 1;
CHECK TABLE d.t SETTINGS check_query_single_value_result = 1;
SQL
