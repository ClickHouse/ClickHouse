-- A horizontal merge writes the columns that hold only defaults in every source part without reading them.
-- The merged data must be the same as before the merge.

-- A part without statistics makes a merge that keeps all rows read every column.
SET materialize_statistics_on_insert = 1;
-- One part per INSERT.
SET max_block_size = 65536, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0, max_insert_threads = 1;

DROP TABLE IF EXISTS hashes;
CREATE TABLE hashes (c String, h UInt64) ENGINE = Memory;

SELECT 'MergeTree';

DROP TABLE IF EXISTS t_merge;
CREATE TABLE t_merge
(
    k UInt64, v UInt64, e_u UInt64, e_s String, e_f Float64, e_i UInt32, x_one UInt64,
    e_n Nullable(UInt32), e_lc LowCardinality(String), e_t Tuple(a UInt64, b String),
    INDEX idx_e_i e_i TYPE minmax
)
ENGINE = MergeTree ORDER BY k
SETTINGS enable_vertical_merge_algorithm = 0, ratio_of_defaults_for_sparse_serialization = 0.9,
    compute_exact_num_defaults_for_sparse_columns = 1, nullable_serialization_version = 'basic',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 1500;

SYSTEM STOP MERGES t_merge;
INSERT INTO t_merge (k, v) SELECT number * 3, number FROM numbers(1000);
INSERT INTO t_merge (k, v, x_one) SELECT number * 3 + 1, number, number = 500 ? 7 : 0 FROM numbers(1000);
INSERT INTO t_merge (k, v) SELECT number * 3 + 2, number FROM numbers(1000);
SELECT groupArray(part_type) FROM system.parts WHERE database = currentDatabase() AND table = 't_merge' AND active;
INSERT INTO hashes SELECT 'merge', sum(cityHash64(toString(tuple(*)))) FROM t_merge;
SYSTEM START MERGES t_merge;
OPTIMIZE TABLE t_merge FINAL;
SELECT groupArray(part_type) FROM system.parts WHERE database = currentDatabase() AND table = 't_merge' AND active;
SELECT (SELECT h FROM hashes WHERE c = 'merge') = sum(cityHash64(toString(tuple(*)))), count(), countIf(x_one != 0) FROM t_merge;
SELECT count() FROM t_merge WHERE e_i = 0 SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'idx_e_i';

SELECT 'ReplacingMergeTree';

DROP TABLE IF EXISTS t_replacing;
CREATE TABLE t_replacing
(
    k UInt64, v UInt64, e_u UInt64, e_s String, e_f Float64, e_i UInt32, x_one UInt64,
    e_n Nullable(UInt32), e_lc LowCardinality(String), e_t Tuple(a UInt64, b String),
    INDEX idx_e_i e_i TYPE minmax
)
ENGINE = ReplacingMergeTree ORDER BY k
SETTINGS enable_vertical_merge_algorithm = 0, ratio_of_defaults_for_sparse_serialization = 0.9,
    compute_exact_num_defaults_for_sparse_columns = 1, nullable_serialization_version = 'basic',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 1500;

SYSTEM STOP MERGES t_replacing;
INSERT INTO t_replacing (k, v) SELECT number * 2, number FROM numbers(1000);
INSERT INTO t_replacing (k, v, x_one) SELECT number * 2, number + 1000, number = 500 ? 7 : 0 FROM numbers(1000);
INSERT INTO t_replacing (k, v) SELECT number * 2 + 1, number FROM numbers(1000);
SELECT groupArray(part_type) FROM system.parts WHERE database = currentDatabase() AND table = 't_replacing' AND active;
INSERT INTO hashes SELECT 'replacing', sum(cityHash64(toString(tuple(*)))) FROM t_replacing FINAL;
SYSTEM START MERGES t_replacing;
OPTIMIZE TABLE t_replacing FINAL;
SELECT groupArray(part_type) FROM system.parts WHERE database = currentDatabase() AND table = 't_replacing' AND active;
SELECT (SELECT h FROM hashes WHERE c = 'replacing') = sum(cityHash64(toString(tuple(*)))), count(), countIf(x_one != 0) FROM t_replacing;
SELECT count() FROM t_replacing WHERE e_i = 0 SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'idx_e_i';

SELECT 'TTL';

DROP TABLE IF EXISTS t_ttl;
-- With vertical_merge_optimize_ttl_delete = 0 the TTL inputs are not merging columns, and the TTL step reads e_ttl.
CREATE TABLE t_ttl (k UInt64, v UInt64, d Date, e_ttl UInt8, e_u UInt64, e_s String)
ENGINE = MergeTree ORDER BY k
TTL d + INTERVAL 1 DAY DELETE WHERE e_ttl = 0
SETTINGS enable_vertical_merge_algorithm = 0, ratio_of_defaults_for_sparse_serialization = 0.9,
    compute_exact_num_defaults_for_sparse_columns = 1, nullable_serialization_version = 'basic',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 1500, vertical_merge_optimize_ttl_delete = 0;

SYSTEM STOP MERGES t_ttl;
INSERT INTO t_ttl (k, v, d) SELECT number * 3, number, number % 2 ? '2000-01-01' : '2100-01-01' FROM numbers(1000);
INSERT INTO t_ttl (k, v, d) SELECT number * 3 + 1, number, number % 2 ? '2000-01-01' : '2100-01-01' FROM numbers(1000);
INSERT INTO t_ttl (k, v, d) SELECT number * 3 + 2, number, number % 2 ? '2000-01-01' : '2100-01-01' FROM numbers(1000);
SELECT groupArray(part_type) FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl' AND active;
INSERT INTO hashes SELECT 'ttl', sum(cityHash64(toString(tuple(*)))) FROM t_ttl WHERE d > '2050-01-01';
SYSTEM START MERGES t_ttl;
OPTIMIZE TABLE t_ttl FINAL;
SELECT groupArray(part_type) FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl' AND active;
SELECT (SELECT h FROM hashes WHERE c = 'ttl') = sum(cityHash64(toString(tuple(*)))), count() FROM t_ttl;

SELECT 'Text index';

DROP TABLE IF EXISTS t_text;
CREATE TABLE t_text (k UInt64, v UInt64, e_txt String, e_s String)
ENGINE = MergeTree ORDER BY k
SETTINGS enable_vertical_merge_algorithm = 0, ratio_of_defaults_for_sparse_serialization = 0.9,
    compute_exact_num_defaults_for_sparse_columns = 1, nullable_serialization_version = 'basic',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 1500;

SYSTEM STOP MERGES t_text;
INSERT INTO t_text (k, v) SELECT number * 3, number FROM numbers(1000);
INSERT INTO t_text (k, v) SELECT number * 3 + 1, number FROM numbers(1000);
INSERT INTO t_text (k, v) SELECT number * 3 + 2, number FROM numbers(1000);
-- Not materialized: the merge builds the index from the data it reads.
ALTER TABLE t_text ADD INDEX idx_txt e_txt TYPE text(tokenizer = 'splitByNonAlpha');
SELECT groupArray(part_type) FROM system.parts WHERE database = currentDatabase() AND table = 't_text' AND active;
INSERT INTO hashes SELECT 'text', sum(cityHash64(toString(tuple(*)))) FROM t_text;
SYSTEM START MERGES t_text;
OPTIMIZE TABLE t_text FINAL;
SELECT groupArray(part_type) FROM system.parts WHERE database = currentDatabase() AND table = 't_text' AND active;
SELECT (SELECT h FROM hashes WHERE c = 'text') = sum(cityHash64(toString(tuple(*)))), count() FROM t_text;
SELECT count() FROM t_text WHERE hasToken(e_txt, 'x');
SELECT count() FROM t_text WHERE e_s = '';

SELECT 'Nullable with allow_sparse';

DROP TABLE IF EXISTS t_nullable;
CREATE TABLE t_nullable (k UInt64, v UInt64, e_n Nullable(UInt32), e_u UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS enable_vertical_merge_algorithm = 0, ratio_of_defaults_for_sparse_serialization = 0.9,
    compute_exact_num_defaults_for_sparse_columns = 1, serialization_info_version = 'with_types', nullable_serialization_version = 'allow_sparse',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 1500;

SYSTEM STOP MERGES t_nullable;
INSERT INTO t_nullable (k, v) SELECT number * 3, number FROM numbers(1000);
-- All NULL, with values under the NULLs.
INSERT INTO t_nullable (k, v, e_n) SELECT number * 3 + 1, number, if(number < 2000, NULL, toUInt32(number) + 1) FROM numbers(1000);
INSERT INTO t_nullable (k, v) SELECT number * 3 + 2, number FROM numbers(1000);
SELECT groupArray(part_type) FROM system.parts WHERE database = currentDatabase() AND table = 't_nullable' AND active;
INSERT INTO hashes SELECT 'nullable', sum(cityHash64(toString(tuple(*)))) FROM t_nullable;
SYSTEM START MERGES t_nullable;
OPTIMIZE TABLE t_nullable FINAL;
SELECT groupArray(part_type) FROM system.parts WHERE database = currentDatabase() AND table = 't_nullable' AND active;
SELECT (SELECT h FROM hashes WHERE c = 'nullable') = sum(cityHash64(toString(tuple(*)))), count(), count(e_n) FROM t_nullable;

SELECT 'Skipped columns';
SET max_rows_to_read = 0; -- system.text_log can be big
SYSTEM FLUSH LOGS text_log;
SELECT t.name, arraySort(groupUniqArray(toUInt32(extract(l.message, 'Writing (\\d+) columns'))))
FROM system.tables AS t
INNER JOIN
(
    SELECT splitByString('::', query_id)[1] AS table_uuid, message
    FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
        AND message_format_string = 'Writing {} columns that have only default values in all parts without merging them'
) AS l ON toString(t.uuid) = l.table_uuid
WHERE t.database = currentDatabase()
GROUP BY t.name
ORDER BY t.name;

DROP TABLE t_merge;
DROP TABLE t_replacing;
DROP TABLE t_ttl;
DROP TABLE t_text;
DROP TABLE t_nullable;
DROP TABLE hashes;
