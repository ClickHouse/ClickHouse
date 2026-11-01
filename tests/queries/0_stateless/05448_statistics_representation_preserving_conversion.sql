-- Tags: no-parallel-replicas
-- no-parallel-replicas: the test checks EXPLAIN of plans with reading steps replaced by prepared sources.

-- Column statistics of a part that stores a column with a type different from the current metadata
-- stay usable when the conversion keeps the representation (an extended `Enum`), and are not used
-- when a read converts the values (a narrowing `MODIFY COLUMN` whose mutation was killed).

SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET use_statistics_for_min_max_aggregation = 1, use_statistics_for_part_pruning = 1;
SET enable_analyzer = 1;
SET optimize_aggregation_in_order = 0, force_aggregation_in_order = 0;
SET aggregate_functions_null_for_empty = 0;

DROP TABLE IF EXISTS t_05448;
CREATE TABLE t_05448 (e Enum8('a' = 1, 'b' = 2) STATISTICS(basic), x Int64 STATISTICS(basic))
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_05448 SELECT if(number % 2, 'a', 'b'), number * 100 FROM numbers(10) SETTINGS materialize_statistics_on_insert = 1;

-- Extending the enum does not rewrite the part, which keeps the old enum type.
ALTER TABLE t_05448 MODIFY COLUMN e Enum8('a' = 1, 'b' = 2, 'c' = 3) STATISTICS(basic) SETTINGS mutations_sync = 2;

SELECT 'extended enum';
SELECT min(e), max(e) FROM t_05448;
SELECT count() FROM (EXPLAIN SELECT min(e), max(e) FROM t_05448) WHERE explain LIKE '%_statistics_min_max_projection%';
SELECT count() FROM t_05448 WHERE e = 'c';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_05448 WHERE e = 'c') WHERE explain LIKE '%Parts: 0/1%';

-- A narrowing alter converts the values on read even after its mutation is killed.
SYSTEM STOP MERGES t_05448;
ALTER TABLE t_05448 MODIFY COLUMN x Int8 STATISTICS(basic) SETTINGS mutations_sync = 0, alter_sync = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_05448' SYNC FORMAT Null;

SELECT 'killed narrowing alter';
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_05448' AND NOT is_done;
SELECT min(x), max(x) FROM t_05448;
SELECT min(x), max(x) FROM t_05448 SETTINGS use_statistics_for_min_max_aggregation = 0;
SELECT count() FROM (EXPLAIN SELECT min(x), max(x) FROM t_05448) WHERE explain LIKE '%_statistics_min_max_projection%';
SELECT count() FROM t_05448 WHERE x < 0;

DROP TABLE t_05448;
