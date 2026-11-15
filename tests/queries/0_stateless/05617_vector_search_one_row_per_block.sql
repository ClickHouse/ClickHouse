-- Tags: no-parallel-replicas
-- (the two-stage quantized codes rewrite is disabled under parallel replicas, so the lazy read of the vectors does not happen.)

-- With `merge_tree_use_one_row_per_block_layout`, the second stage of lazy materialization reads only the shortlisted rows
-- of the vector column, not whole granules. This needs one row per block, stored without compression.
-- The results must be the same as with the usual readers.

SET enable_quantized_codec = 1;
SET vector_search_use_quantized_codes = 1;
SET vector_search_index_fetch_multiplier = 10;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000000;
-- The setting has no effect with the columns cache or the uncompressed cache.
SET use_columns_cache = 0;
SET use_uncompressed_cache = 0;

DROP TABLE IF EXISTS tab_one_row_per_block;
DROP TABLE IF EXISTS tab_several_rows_per_block;
DROP TABLE IF EXISTS tab_sizes;
DROP TABLE IF EXISTS tab_other_block_sizes;
DROP TABLE IF EXISTS tab_adaptive;
DROP TABLE IF EXISTS tab_merged;
DROP TABLE IF EXISTS tab_resized;
DROP TABLE IF EXISTS tab_long_name;
DROP TABLE IF EXISTS tab_lz4;

-- 64 `Float32` elements are 256 bytes, so every row is one block.
CREATE TABLE tab_one_row_per_block
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('int8', 64)) SETTINGS (min_compress_block_size = 256, max_compress_block_size = 256)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_compress_block_size = 65536, max_compress_block_size = 1048576,
    index_granularity = 8192, index_granularity_bytes = 10485760;

SYSTEM STOP MERGES tab_one_row_per_block;

INSERT INTO tab_one_row_per_block SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(2000);

SELECT 'one part, setting on';
WITH (SELECT vec FROM tab_one_row_per_block WHERE id = 123) AS ref
SELECT id FROM tab_one_row_per_block ORDER BY L2Distance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = '05617_one_part_on';

SELECT 'one part, setting off';
WITH (SELECT vec FROM tab_one_row_per_block WHERE id = 123) AS ref
SELECT id FROM tab_one_row_per_block ORDER BY L2Distance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 0, log_comment = '05617_one_part_off';

-- The column-level block sizes apply only to the stream of the array elements of a `Quantized` column. The quantized
-- codes, 68 bytes per row, are stored in blocks of the default size, not in a block per 4 rows. The number of blocks
-- decompressed is counted, so the read is not split.
SELECT count() FROM tab_one_row_per_block WHERE NOT ignore(vec.quantized)
SETTINGS log_comment = '05617_codes', max_threads = 1, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

-- A Compact part is read with the usual readers. The rows of both parts must keep their order.
ALTER TABLE tab_one_row_per_block MODIFY SETTING min_bytes_for_wide_part = 1000000000;
INSERT INTO tab_one_row_per_block SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(2000, 2000);

SELECT 'two parts, setting on';
WITH (SELECT vec FROM tab_one_row_per_block WHERE id = 123) AS ref
SELECT id FROM tab_one_row_per_block ORDER BY L2Distance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = '05617_two_parts_on';

SELECT 'two parts, setting off';
WITH (SELECT vec FROM tab_one_row_per_block WHERE id = 123) AS ref
SELECT id FROM tab_one_row_per_block ORDER BY L2Distance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 0, log_comment = '05617_two_parts_off';

-- Without the column-level settings, the blocks hold several rows, and the usual readers are used.
CREATE TABLE tab_several_rows_per_block
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('int8', 64))
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO tab_several_rows_per_block SELECT * FROM tab_one_row_per_block WHERE id < 2000;

SELECT 'several rows per block, setting on';
WITH (SELECT vec FROM tab_several_rows_per_block WHERE id = 123) AS ref
SELECT id FROM tab_several_rows_per_block ORDER BY L2Distance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = '05617_several_rows_per_block_on';

-- The queries below are sorted by an expression other than the sorting key, so that the vectors are read lazily.
-- Rows of 63 and 65 elements: the file of the elements has the size of one block of 64 elements per row, but the rows
-- cross the blocks. The writer sees rows of different sizes and does not record the layout, so the usual readers are used.
CREATE TABLE tab_sizes
(
    id UInt32,
    vec Array(Float32) CODEC(NONE) SETTINGS (min_compress_block_size = 256, max_compress_block_size = 256)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO tab_sizes SELECT number, arrayMap(j -> toFloat32(number * 100 + j), range(if(number % 2 = 0, 63, 65)))
FROM numbers(2000);

SELECT 'different sizes, setting on';
SELECT id, length(vec), vec[1], vec[-1] FROM tab_sizes ORDER BY sipHash64(id) LIMIT 5
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = '05617_sizes_on';

SELECT 'different sizes, setting off';
SELECT id, length(vec), vec[1], vec[-1] FROM tab_sizes ORDER BY sipHash64(id) LIMIT 5
SETTINGS merge_tree_use_one_row_per_block_layout = 0, log_comment = '05617_sizes_off';

-- The same query on the Wide part of the table where every row is one block is read this way.
SELECT 'same sizes, setting on';
SELECT id, length(vec), vec[1] FROM tab_one_row_per_block ORDER BY sipHash64(id) LIMIT 5
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = '05617_same_sizes_on';

-- A part written with other block sizes than the column has now: blocks of 260 and 252 bytes, which add up to the size of
-- one block per row. Neither the settings nor the sizes of the files show it, but the writer did not record the layout,
-- so the part is read as usual.
CREATE TABLE tab_other_block_sizes
(
    id UInt32,
    vec Array(Float32) CODEC(NONE) SETTINGS (min_compress_block_size = 1, max_compress_block_size = 260)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 2, index_granularity_bytes = 0;

INSERT INTO tab_other_block_sizes SELECT number, arrayMap(j -> toFloat32(number * 100 + j), range(64)) FROM numbers(2000);
ALTER TABLE tab_other_block_sizes MODIFY COLUMN vec Array(Float32) CODEC(NONE) SETTINGS (min_compress_block_size = 256, max_compress_block_size = 256);

SELECT 'other block sizes, setting on';
SELECT id, vec[1] FROM tab_other_block_sizes ORDER BY sipHash64(id) LIMIT 5
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = '05617_other_block_sizes_on';

SELECT 'other block sizes, setting off';
SELECT id, vec[1] FROM tab_other_block_sizes ORDER BY sipHash64(id) LIMIT 5
SETTINGS merge_tree_use_one_row_per_block_layout = 0;

-- A part written with an adaptive write buffer, whose blocks grow from 100 bytes. After the column-level settings are
-- changed, the files have the sizes of one block of 3200 bytes per row, and the position computed for row 1 would be a
-- block that starts in row 0. The writer did not record the layout, so the part is read as usual.
CREATE TABLE tab_adaptive
(
    id UInt32,
    vec Array(Float32) CODEC(NONE) SETTINGS (min_compress_block_size = 1, max_compress_block_size = 6400)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 2, index_granularity_bytes = 0,
    min_columns_to_activate_adaptive_write_buffer = 1, adaptive_write_buffer_initial_size = 100;

INSERT INTO tab_adaptive SELECT number, arrayMap(x -> toFloat32(number), range(800)) FROM numbers(12);
ALTER TABLE tab_adaptive MODIFY COLUMN vec Array(Float32) CODEC(NONE) SETTINGS (min_compress_block_size = 3200, max_compress_block_size = 3200);

SELECT 'adaptive write buffer, setting on';
SELECT id, vec[1], vec[-1], length(vec) FROM tab_adaptive ORDER BY abs(toInt64(id) - 1) LIMIT 1
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = '05617_adaptive_on';

SELECT 'adaptive write buffer, setting off';
SELECT id, vec[1], vec[-1], length(vec) FROM tab_adaptive ORDER BY abs(toInt64(id) - 1) LIMIT 1
SETTINGS merge_tree_use_one_row_per_block_layout = 0;

-- A merge writes the layout of the merged part again, a mutation of another column keeps the file of the vectors and its
-- record, and a mutation of the vectors writes both again, so all these parts are read this way.
CREATE TABLE tab_merged
(
    id UInt32,
    vec Array(Float32) CODEC(NONE) SETTINGS (min_compress_block_size = 256, max_compress_block_size = 256),
    x UInt8
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO tab_merged SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64)), 0
FROM numbers(1000);
INSERT INTO tab_merged SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64)), 0
FROM numbers(1000, 1000);
OPTIMIZE TABLE tab_merged FINAL;

SELECT 'merged part, setting on';
SELECT id, vec[1] FROM tab_merged ORDER BY sipHash64(id) LIMIT 5
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = '05617_merged_on';

ALTER TABLE tab_merged UPDATE x = 1 WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'mutated part, setting on';
SELECT id, vec[1], x FROM tab_merged ORDER BY sipHash64(id) LIMIT 5
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = '05617_mutated_on';

ALTER TABLE tab_merged UPDATE vec = arrayMap(v -> v + 1, vec) WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'mutated vectors, setting on';
SELECT id, vec[1], x FROM tab_merged ORDER BY sipHash64(id) LIMIT 5
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = '05617_vec_mutated_on';

SELECT 'mutated vectors, setting off';
SELECT id, vec[1], x FROM tab_merged ORDER BY sipHash64(id) LIMIT 5
SETTINGS merge_tree_use_one_row_per_block_layout = 0;

-- A mutation that changes the sizes of the arrays but not the bytes of the elements: rows of 64 ones become rows of 63 and
-- 65 ones, so the file of the elements is the same, but the rows cross the blocks. The record of the source part must
-- not be used.
CREATE TABLE tab_resized
(
    id UInt32,
    vec Array(Float32) CODEC(NONE) SETTINGS (min_compress_block_size = 256, max_compress_block_size = 256)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO tab_resized SELECT number, arrayMap(j -> toFloat32(1), range(64)) FROM numbers(2000);
ALTER TABLE tab_resized UPDATE vec = arrayMap(j -> toFloat32(1), range(if(id % 2 = 0, 63, 65))) WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'resized arrays, setting on';
SELECT id, length(vec), arraySum(vec) FROM tab_resized ORDER BY sipHash64(id) LIMIT 5
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = '05617_resized_on';

SELECT 'resized arrays, setting off';
SELECT id, length(vec), arraySum(vec) FROM tab_resized ORDER BY sipHash64(id) LIMIT 5
SETTINGS merge_tree_use_one_row_per_block_layout = 0;

-- The name of the record does not depend on the name of the column, so a column with a long name, kept as is with a large
-- `max_file_name_length`, can be inserted and is read this way.
CREATE TABLE tab_long_name
(
    id UInt32,
    vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv Array(Float32) CODEC(NONE) SETTINGS (min_compress_block_size = 256, max_compress_block_size = 256)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, max_file_name_length = 255;

INSERT INTO tab_long_name SELECT number, arrayMap(j -> toFloat32(number * 100 + j), range(64)) FROM numbers(2000)
SETTINGS merge_tree_use_one_row_per_block_layout = 0;

SELECT 'long column name, setting on';
SELECT id, vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv[1] FROM tab_long_name ORDER BY sipHash64(id) LIMIT 5
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = '05617_long_name_on';

-- A codec that compresses is not supported, even when every compressed block happens to have the size of a row,
-- as with these values and `LZ4`: the file then has the sizes of one block per row stored without compression.
CREATE TABLE tab_lz4
(
    id UInt32,
    vec Array(Int8) CODEC(LZ4) SETTINGS (min_compress_block_size = 256, max_compress_block_size = 256)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO tab_lz4 SELECT number, CAST([6,-16,57,-55,72,6,-16,57,-55,72,75,16,31,69,118,-27,-45,104,47,60,10,-50,-82,101,105,-87,-56,-70,126,107,111,-43,30,26,92,-74,68,104,75,-64,15,44,-32,111,106,-35,-68,99,-87,112,-10,21,-122,-84,-106,-85,-54,-10,-95,109,48,-100,90,-105,47,88,98,14,-65,48,-52,-111,-100,100,58,-24,103,-86,-29,-50,24,60,-119,50,61,99,102,-85,83,77,-13,29,-1,-114,-66,-5,19,57,-8,70,-2,12,-126,-6,-16,-78,121,5,49,-64,-112,-28,90,23,67,63,25,105,98,-104,94,-24,-51,116,99,-20,-99,-17,-90,68,-66,63,-79,98,-107,-62,85,21,60,-39,106,72,125,-62,-99,104,-39,-99,124,-119,19,16,-15,-116,-124,103,-34,-70,107,-51,17,-29,-60,101,122,109,-92,-80,-104,-68,57,110,33,127,-11,-1,-25,-114,92,-43,17,34,-9,78,-104,-14,2,0,-115,-62,-117,-111,-103,125,-99,-50,119,122,72,-93,49,96,10,-65,-4,-100,-16,-124,-98,-58,-53,109,113,-46,24,124,122,24,-46,-91,-117,55,31,-89,127,-5,-38,72,24,-98,-96,22,-47,115,-31,72,39,11,-4,-14,-31,-117,-11,-12,1,-83,120,86,45,-18,-73,29,-56,-92,-57,103], 'Array(Int8)') FROM numbers(2000);

SELECT 'compressed, setting on';
SELECT id, vec[1], length(vec) FROM tab_lz4 ORDER BY sipHash64(id) LIMIT 5
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = '05617_lz4_on';

SELECT 'compressed, setting off';
SELECT id, vec[1], length(vec) FROM tab_lz4 ORDER BY sipHash64(id) LIMIT 5
SETTINGS merge_tree_use_one_row_per_block_layout = 0;

SYSTEM FLUSH LOGS query_log;

SELECT 'codes in few blocks', ProfileEvents['CompressedReadBufferBlocks'] < 10
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05617_codes';

-- The shortlist has 10 * 10 = 100 rows. With one part with the layout, all of them are read this way.
-- With a second part that is Compact, only the rows of the first part are. Without the layout or with the setting off, none are.
-- The queries sorted by `sipHash64(id)` read the 5 rows of their result.
SELECT log_comment, ProfileEvents['MergeTreeOneRowPerBlockReadRows'] AS rows_read_by_blocks
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05617_one_part_on', '05617_one_part_off', '05617_two_parts_on', '05617_two_parts_off', '05617_several_rows_per_block_on',
        '05617_sizes_on', '05617_sizes_off', '05617_same_sizes_on', '05617_lz4_on', '05617_other_block_sizes_on', '05617_adaptive_on',
        '05617_merged_on', '05617_mutated_on', '05617_vec_mutated_on', '05617_resized_on', '05617_long_name_on')
ORDER BY log_comment;

DROP TABLE tab_one_row_per_block;
DROP TABLE tab_several_rows_per_block;
DROP TABLE tab_sizes;
DROP TABLE tab_other_block_sizes;
DROP TABLE tab_adaptive;
DROP TABLE tab_merged;
DROP TABLE tab_resized;
DROP TABLE tab_long_name;
DROP TABLE tab_lz4;
