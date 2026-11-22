-- Merges and inserts keep `String` columns with dictionary granules equal to a `with_size_stream` twin. A
-- horizontal and a vertical merge of overlapping parts and a merge of one part into one take the columns as
-- they are read; an `INSERT SELECT` into a `MergeTree` table does as well, while a `Memory` table and a
-- `ReplacingMergeTree` table with `optimize_on_insert` get them materialized. A vertical merge into a sparse
-- column materializes its sources, and an insert into a `MergeTree` table materializes the lazily replicated
-- `Array` columns of a `JOIN`.

DROP TABLE IF EXISTS t_plain;
DROP TABLE IF EXISTS t_horizontal;
DROP TABLE IF EXISTS t_vertical;
DROP TABLE IF EXISTS t_copy;
DROP TABLE IF EXISTS t_memory;
DROP TABLE IF EXISTS t_replacing;

CREATE TABLE t_plain (id UInt64, low String, mixed String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1, serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream';

CREATE TABLE t_horizontal (id UInt64, low String, mixed String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1, serialization_info_version = 'with_types', string_serialization_version = 'with_dictionaries',
    vertical_merge_algorithm_min_rows_to_activate = 1000000000;

CREATE TABLE t_vertical (id UInt64, low String, mixed String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1, serialization_info_version = 'with_types', string_serialization_version = 'with_dictionaries',
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1;

SYSTEM STOP MERGES t_horizontal;
SYSTEM STOP MERGES t_vertical;

-- `mixed` alternates between granules of 5 values and granules of unique values.
INSERT INTO t_plain SELECT number, 'l' || toString(number % 7), if(intDiv(number, 64) % 2 = 0, toString(number % 5), toString(cityHash64(number))) FROM numbers(3000);
INSERT INTO t_horizontal SELECT number, 'l' || toString(number % 7), if(intDiv(number, 64) % 2 = 0, toString(number % 5), toString(cityHash64(number))) FROM numbers(3000) WHERE number % 3 = 0;
INSERT INTO t_horizontal SELECT number, 'l' || toString(number % 7), if(intDiv(number, 64) % 2 = 0, toString(number % 5), toString(cityHash64(number))) FROM numbers(3000) WHERE number % 3 != 0;
INSERT INTO t_vertical SELECT * FROM t_plain WHERE id % 3 = 0;
INSERT INTO t_vertical SELECT * FROM t_plain WHERE id % 3 != 0;

SYSTEM START MERGES t_horizontal;
SYSTEM START MERGES t_vertical;
OPTIMIZE TABLE t_horizontal FINAL;
OPTIMIZE TABLE t_vertical FINAL;

SELECT 'horizontal merge', (SELECT sum(cityHash64(id, low, mixed)) FROM t_plain) = (SELECT sum(cityHash64(id, low, mixed)) FROM t_horizontal);
SELECT 'vertical merge', (SELECT sum(cityHash64(id, low, mixed)) FROM t_plain) = (SELECT sum(cityHash64(id, low, mixed)) FROM t_vertical);

OPTIMIZE TABLE t_horizontal FINAL;
SELECT 'one part into one', (SELECT sum(cityHash64(id, low, mixed)) FROM t_plain) = (SELECT sum(cityHash64(id, low, mixed)) FROM t_horizontal);

SYSTEM FLUSH LOGS part_log;
SELECT table, merge_algorithm FROM system.part_log
WHERE database = currentDatabase() AND event_type = 'MergeParts' AND table IN ('t_horizontal', 't_vertical')
GROUP BY ALL ORDER BY ALL;

CREATE TABLE t_copy (id UInt64, low String, mixed String)
ENGINE = MergeTree ORDER BY (low, id)
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1, serialization_info_version = 'with_types', string_serialization_version = 'with_dictionaries';
CREATE TABLE t_memory (id UInt64, low String, mixed String) ENGINE = Memory;
CREATE TABLE t_replacing (id UInt64, low String, mixed String)
ENGINE = ReplacingMergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1, serialization_info_version = 'with_types', string_serialization_version = 'with_dictionaries';

INSERT INTO t_copy SELECT * FROM t_horizontal;
INSERT INTO t_memory SELECT * FROM t_horizontal;
INSERT INTO t_replacing SELECT * FROM t_horizontal UNION ALL SELECT * FROM t_horizontal SETTINGS optimize_on_insert = 1;

SELECT 'insert into MergeTree', (SELECT sum(cityHash64(id, low, mixed)) FROM t_plain) = (SELECT sum(cityHash64(id, low, mixed)) FROM t_copy);
SELECT 'insert into Memory', (SELECT sum(cityHash64(id, low, mixed)) FROM t_plain) = (SELECT sum(cityHash64(id, low, mixed)) FROM t_memory);
SELECT 'insert with optimize_on_insert', (SELECT sum(cityHash64(id, low, mixed)) FROM t_plain) = (SELECT sum(cityHash64(id, low, mixed)) FROM t_replacing);

-- The merged part stores the column as sparse, while the first part has dictionary granules for it.
DROP TABLE IF EXISTS t_sparse_plain;
DROP TABLE IF EXISTS t_sparse_dict;
CREATE TABLE t_sparse_plain (id UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 0.9, serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream';
CREATE TABLE t_sparse_dict (id UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 0.9, serialization_info_version = 'with_types', string_serialization_version = 'with_dictionaries',
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1;
SYSTEM STOP MERGES t_sparse_dict;
INSERT INTO t_sparse_plain SELECT number, if(number < 1000 AND number % 2 = 0, 'x' || toString(number % 3), '') FROM numbers(20000);
INSERT INTO t_sparse_dict SELECT number, if(number % 2 = 0, 'x' || toString(number % 3), '') FROM numbers(1000);
INSERT INTO t_sparse_dict SELECT number, '' FROM numbers(1000, 19000);
SYSTEM START MERGES t_sparse_dict;
OPTIMIZE TABLE t_sparse_dict FINAL;
SELECT 'vertical merge into a sparse column', (SELECT sum(cityHash64(id, s)) FROM t_sparse_plain) = (SELECT sum(cityHash64(id, s)) FROM t_sparse_dict),
    (SELECT serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sparse_dict' AND active AND column = 's');
DROP TABLE t_sparse_plain;
DROP TABLE t_sparse_dict;

-- Each left row matches five right rows, so the `JOIN` replicates the left columns lazily.
DROP TABLE IF EXISTS t_join_left;
DROP TABLE IF EXISTS t_join_right;
DROP TABLE IF EXISTS t_arrays;
CREATE TABLE t_join_left (k UInt64, arr Array(UInt64)) ENGINE = Memory;
CREATE TABLE t_join_right (k UInt64) ENGINE = Memory;
CREATE TABLE t_arrays (k UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_join_left SELECT number % 10, range(number % 5) FROM numbers(100);
INSERT INTO t_join_right SELECT number % 10 FROM numbers(50);
INSERT INTO t_arrays SELECT l.k, l.arr FROM t_join_left AS l INNER JOIN t_join_right AS r ON l.k = r.k SETTINGS enable_lazy_columns_replication = 1;
SELECT 'insert of lazily replicated arrays', count(), sum(length(arr)) FROM t_arrays;
DROP TABLE t_join_left;
DROP TABLE t_join_right;
DROP TABLE t_arrays;

DROP TABLE t_plain;
DROP TABLE t_horizontal;
DROP TABLE t_vertical;
DROP TABLE t_copy;
DROP TABLE t_memory;
DROP TABLE t_replacing;
