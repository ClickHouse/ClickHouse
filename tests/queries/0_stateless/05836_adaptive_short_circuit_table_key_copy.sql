-- The table key expressions are built as plain `ExpressionActions` even with `enable_adaptive_short_circuit_lazy_execution`,
-- because they are shared: e.g. every sink of a parallel insert executes the sorting key of the same metadata snapshot.
-- Copying the metadata (`IStorage::setInMemoryMetadata`) clones them, and the clones must stay plain: a stateful
-- `AdaptiveExpressionActions` executed by several threads at once is a data race, caught by the sanitizer builds.

SET short_circuit_function_evaluation = 'force_enable';
SET enable_adaptive_short_circuit_lazy_execution = 1;

DROP TABLE IF EXISTS t_adaptive_key_copy;

CREATE TABLE t_adaptive_key_copy (a UInt64, b UInt64)
ENGINE = MergeTree
ORDER BY (if(a % 3 = 0, intDiv(b, a % 5 + 1), b), a)
PARTITION BY if(b % 2 = 0, intDiv(a, b + 1) % 2, 2)
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO t_adaptive_key_copy SELECT number, number % 7 FROM numbers_mt(400000)
SETTINGS max_insert_threads = 8, max_threads = 8, max_block_size = 5000, min_insert_block_size_rows = 5000, min_insert_block_size_bytes = 0;

SELECT count(), sum(a), sum(b) FROM t_adaptive_key_copy;

DROP TABLE t_adaptive_key_copy;
