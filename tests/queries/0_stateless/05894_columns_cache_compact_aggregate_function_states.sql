-- Tags: no-parallel
-- no-parallel: the read relies on the admission to the columns cache, which `SYSTEM DROP COLUMNS CACHE` resets and the
-- tests that fill the cache at the same time make probabilistic.

-- Reading a compact part with the columns cache copies its granules into a column that becomes a cache entry. The copy of
-- an `AggregateFunction` column, also nested in a `Tuple` or an `Array`, must own its states: otherwise it refers to the
-- column being read, which is then shared while the next granules of the block are read into it.

SET use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1;
SET max_threads = 1, enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_cc_compact_states;

CREATE TABLE t_cc_compact_states
(
    k UInt64,
    t Tuple(UInt64, AggregateFunction(sum, UInt64)),
    a Array(AggregateFunction(sum, UInt64))
)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, index_granularity = 4,
    index_granularity_bytes = '10Mi', ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_cc_compact_states
SELECT number, (number, arrayReduce('sumState', [number])), [arrayReduce('sumState', [number])] FROM numbers(64);

SELECT part_type, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_cc_compact_states' AND active;

SYSTEM DROP COLUMNS CACHE;

-- Blocks of two granules inside the one stripe of the part: the granules are copied into the cache granule by granule.
SELECT t, a FROM t_cc_compact_states SETTINGS max_block_size = 8 FORMAT Null;

SELECT column, count() > 0 FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_cc_compact_states' GROUP BY column ORDER BY column;

SELECT sumMerge(t.2) FROM t_cc_compact_states;
SELECT sumMerge(arrayJoin(a)) FROM t_cc_compact_states;

DROP TABLE t_cc_compact_states;
