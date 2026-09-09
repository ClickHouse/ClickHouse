-- Tags: no-parallel-replicas, no-replicated-database
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.
-- no-replicated-database: EXPLAIN output differs for replicated database.

-- optimize_extract_common_expressions rewrites `(A AND X) OR A` (logically equivalent to `A`)
-- into a conjunction wrapped in a CAST that restores the original (Nullable) result type,
-- e.g. `_CAST(A, 'Nullable(UInt8)')`. That CAST used to make the primary-key / KeyCondition
-- analysis unable to see through to `A`, so with force_primary_key = 1 the query was
-- incorrectly rejected with INDEX_NOT_USED even though the primary key is trivially usable.

-- { echo }

DROP TABLE IF EXISTS t_extract_common_pk;

CREATE TABLE t_extract_common_pk
(
    k UInt32,
    n Nullable(Int32)
)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 1;

INSERT INTO t_extract_common_pk SELECT number, number FROM numbers(100);

-- (A AND X) OR A, where A = 'k = 1' and X involves a Nullable column (n).
SELECT count() FROM t_extract_common_pk WHERE (k = 1 AND n = 1) OR k = 1 SETTINGS force_primary_key = 1, optimize_extract_common_expressions = 1;

-- Same, with the disjuncts swapped: A OR (A AND X).
SELECT count() FROM t_extract_common_pk WHERE k = 1 OR (k = 1 AND n = 1) SETTINGS force_primary_key = 1, optimize_extract_common_expressions = 1;

-- The primary key must actually be used to prune granules, not merely avoid the exception.
EXPLAIN indexes = 1
SELECT count() FROM t_extract_common_pk WHERE (k = 1 AND n = 1) OR k = 1 SETTINGS optimize_extract_common_expressions = 1;

DROP TABLE t_extract_common_pk;
