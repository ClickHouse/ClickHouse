-- An always-false condition on a key column that is not loaded in the in-memory primary index
-- must prune every granule with `use_lightweight_primary_key_index_analysis = 1`, as with 0.
-- The unique leading `id` keeps only `id` in memory, so `c` has no per-mark values.

DROP TABLE IF EXISTS t_unloaded;
DROP TABLE IF EXISTS t_loaded;
DROP TABLE IF EXISTS t_empty;
DROP TABLE IF EXISTS t_null;

CREATE TABLE t_unloaded (id UInt64, c String) ENGINE = MergeTree ORDER BY (id, c)
SETTINGS index_granularity = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.9, primary_key_lazy_load = 0, use_primary_key_cache = 0;
CREATE TABLE t_loaded (id UInt64, c String) ENGINE = MergeTree ORDER BY (id, c)
SETTINGS index_granularity = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0, primary_key_lazy_load = 0, use_primary_key_cache = 0;
CREATE TABLE t_empty (c String) ENGINE = Null;
CREATE TABLE t_null (c Nullable(String)) ENGINE = Memory;

INSERT INTO t_unloaded SELECT number, toString(number % 10) FROM numbers(100);
INSERT INTO t_loaded SELECT * FROM t_unloaded;
INSERT INTO t_null VALUES (NULL);

-- Premise: `c` is not loaded in memory for `t_unloaded`.
SELECT (SELECT sum(primary_key_bytes_in_memory) FROM system.parts WHERE database = currentDatabase() AND table = 't_unloaded' AND active)
     < (SELECT sum(primary_key_bytes_in_memory) FROM system.parts WHERE database = currentDatabase() AND table = 't_loaded' AND active);

SELECT 'empty set, lw 0', trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM t_unloaded WHERE c IN (SELECT c FROM t_empty)
    SETTINGS use_lightweight_primary_key_index_analysis = 0, use_statistics_for_part_pruning = 0, enable_parallel_replicas = 0)
WHERE explain ILIKE '%Granules: %/%';
SELECT 'empty set, lw 1', trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM t_unloaded WHERE c IN (SELECT c FROM t_empty)
    SETTINGS use_lightweight_primary_key_index_analysis = 1, use_statistics_for_part_pruning = 0, enable_parallel_replicas = 0)
WHERE explain ILIKE '%Granules: %/%';

-- A set built from NULL only is empty.
SELECT 'NULL-only set, lw 0', trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM t_unloaded WHERE c IN (SELECT c FROM t_null)
    SETTINGS use_lightweight_primary_key_index_analysis = 0, use_statistics_for_part_pruning = 0, enable_parallel_replicas = 0, transform_null_in = 0)
WHERE explain ILIKE '%Granules: %/%';
SELECT 'NULL-only set, lw 1', trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM t_unloaded WHERE c IN (SELECT c FROM t_null)
    SETTINGS use_lightweight_primary_key_index_analysis = 1, use_statistics_for_part_pruning = 0, enable_parallel_replicas = 0, transform_null_in = 0)
WHERE explain ILIKE '%Granules: %/%';

SELECT count() FROM t_unloaded WHERE c IN (SELECT c FROM t_empty) SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_unloaded WHERE c NOT IN (SELECT c FROM t_empty) SETTINGS use_lightweight_primary_key_index_analysis = 1;

DROP TABLE t_unloaded;
DROP TABLE t_loaded;
DROP TABLE t_empty;
DROP TABLE t_null;
