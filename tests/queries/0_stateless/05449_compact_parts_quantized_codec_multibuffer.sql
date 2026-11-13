-- Tags: no-parallel-replicas

-- Regression test: a compact part's multi-buffer reader may skip the per-granule seek inside a stripe and rely on the
-- previous granule's read having left the stream at the next granule. The `Quantized(...)` codec writes a codes stream
-- after each granule's array streams, and a full-column read does not consume it, so the array streams were left
-- mid-granule and the next granule's array sizes were read out of the codes blob:
--   Array size at index 0 is too large: 10917579201406537675 (TOO_LARGE_ARRAY_SIZE)
-- The failure needs a compact part, the multi-buffer reader, more than one granule read per stripe, and a full-column
-- (not subcolumn) read: `optimize_functions_to_subcolumns` must stay off so `vec` is not rewritten to `vec.size0`, and
-- the blocks must be small enough that a second reader call happens inside one stripe.

SET enable_quantized_codec = 1;
SET optimize_functions_to_subcolumns = 0;
SET max_block_size = 128;
SET preferred_block_size_bytes = 0;
SET preferred_max_column_in_block_size_bytes = 0;
SET max_threads = 1;
-- The `read` method forces the single-buffer reader, which seeks every granule and cannot hit this bug.
SET local_filesystem_read_method = 'pread_threadpool';
SET merge_tree_compact_parts_min_granules_to_multibuffer_read = 16;

-- With substream marks (the default): the read seeks to the granule's own mark.
DROP TABLE IF EXISTS quantize_multibuffer;
CREATE TABLE quantize_multibuffer (id UInt32, vec Array(Float32) CODEC(Quantized('rabitq', 64)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
         write_marks_for_substreams_in_compact_parts = 1,
         compact_parts_merge_max_bytes_to_prefetch_part = 0,
         compact_parts_max_granules_to_buffer = 128,
         compact_parts_max_bytes_to_buffer = '128M',
         index_granularity = 128, index_granularity_bytes = 10485760;

INSERT INTO quantize_multibuffer SELECT number, arrayMap(i -> toFloat32(number + i), range(64)) FROM numbers(20000);

-- A Wide part cannot hit this bug, so a part-format change would silently make the test vacuous.
SELECT 'marks_part_type', part_type, marks > 16 FROM system.parts
WHERE database = currentDatabase() AND table = 'quantize_multibuffer' AND active;
SELECT 'marks_sum', sum(arraySum(vec)) FROM quantize_multibuffer;
SELECT 'marks_subcolumns', sum(vec.size0), sum(sipHash64(vec.quantized)) > 0 FROM quantize_multibuffer;

DROP TABLE quantize_multibuffer;

-- Without substream marks: there is no mark to seek to, so the granule's codes are consumed instead. A second
-- quantize method also shows the fix is not shape-specific.
DROP TABLE IF EXISTS quantize_multibuffer_nomarks;
CREATE TABLE quantize_multibuffer_nomarks (id UInt32, vec Array(Float32) CODEC(Quantized('int8', 64)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
         write_marks_for_substreams_in_compact_parts = 0,
         compact_parts_merge_max_bytes_to_prefetch_part = 0,
         compact_parts_max_granules_to_buffer = 128,
         compact_parts_max_bytes_to_buffer = '128M',
         index_granularity = 128, index_granularity_bytes = 10485760;

INSERT INTO quantize_multibuffer_nomarks SELECT number, arrayMap(i -> toFloat32(number + i), range(64)) FROM numbers(20000);

SELECT 'nomarks_part_type', part_type, marks > 16 FROM system.parts
WHERE database = currentDatabase() AND table = 'quantize_multibuffer_nomarks' AND active;
SELECT 'nomarks_sum', sum(arraySum(vec)) FROM quantize_multibuffer_nomarks;
-- A partial-granule read: the skipped prefix is read and discarded through the same path.
SELECT 'nomarks_offset', sum(arraySum(vec)) FROM (SELECT vec FROM quantize_multibuffer_nomarks ORDER BY id LIMIT 5000 OFFSET 7003);

DROP TABLE quantize_multibuffer_nomarks;

-- The reported failure was on the vertical merge path, which reads the part through the same reader.
DROP TABLE IF EXISTS quantize_multibuffer_merge;
CREATE TABLE quantize_multibuffer_merge (id UInt32, vec Array(Float32) CODEC(Quantized('rabitq', 64)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
         compact_parts_merge_max_bytes_to_prefetch_part = 0,
         compact_parts_max_granules_to_buffer = 128,
         compact_parts_max_bytes_to_buffer = '128M',
         index_granularity = 128, index_granularity_bytes = 10485760,
         vertical_merge_algorithm_min_rows_to_activate = 1,
         vertical_merge_algorithm_min_columns_to_activate = 1;

INSERT INTO quantize_multibuffer_merge SELECT number, arrayMap(i -> toFloat32(number + i), range(64)) FROM numbers(10000);
INSERT INTO quantize_multibuffer_merge SELECT number + 10000, arrayMap(i -> toFloat32(number + i), range(64)) FROM numbers(10000);
OPTIMIZE TABLE quantize_multibuffer_merge FINAL;

SELECT 'merge_parts', count() FROM system.parts
WHERE database = currentDatabase() AND table = 'quantize_multibuffer_merge' AND active;
SELECT 'merge_sum', sum(arraySum(vec)) FROM quantize_multibuffer_merge;

DROP TABLE quantize_multibuffer_merge;
