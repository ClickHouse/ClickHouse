-- The query condition cache for the `Memory` engine: an `ALTER` of the columns keeps the stored blocks,
-- but can change the values read from them, so the entries written before it must not be used after it.

SET use_query_condition_cache = 1;

DROP TABLE IF EXISTS t_memory_qcc_alter;

SELECT '-- a Nested subcolumn missing in the blocks takes the array sizes from its stored siblings';
CREATE TABLE t_memory_qcc_alter (k UInt64, n Nested(a UInt64)) ENGINE = Memory;
INSERT INTO t_memory_qcc_alter VALUES (1, [10, 20]), (2, [30]);
ALTER TABLE t_memory_qcc_alter ADD COLUMN `n.b` Array(UInt64);
SELECT count() FROM t_memory_qcc_alter PREWHERE empty(n.b);
ALTER TABLE t_memory_qcc_alter DROP COLUMN `n.a`;
SELECT count() FROM t_memory_qcc_alter PREWHERE empty(n.b);
SELECT count() FROM t_memory_qcc_alter PREWHERE empty(n.b) SETTINGS use_query_condition_cache = 0;
DROP TABLE t_memory_qcc_alter;

SELECT '-- the same with the filter of the query, compressed';
CREATE TABLE t_memory_qcc_alter (k UInt64, n Nested(a UInt64)) ENGINE = Memory SETTINGS compress = 1;
INSERT INTO t_memory_qcc_alter VALUES (1, [10, 20]), (2, [30]);
ALTER TABLE t_memory_qcc_alter ADD COLUMN `n.b` Array(UInt64);
SELECT count() FROM t_memory_qcc_alter WHERE empty(n.b) SETTINGS optimize_move_to_prewhere = 0;
ALTER TABLE t_memory_qcc_alter DROP COLUMN `n.a`;
SELECT count() FROM t_memory_qcc_alter WHERE empty(n.b) SETTINGS optimize_move_to_prewhere = 0;
SELECT count() FROM t_memory_qcc_alter WHERE empty(n.b) SETTINGS optimize_move_to_prewhere = 0, use_query_condition_cache = 0;
DROP TABLE t_memory_qcc_alter;

SELECT '-- a cast to the changed type on read';
CREATE TABLE t_memory_qcc_alter (k UInt64, j JSON) ENGINE = Memory;
INSERT INTO t_memory_qcc_alter VALUES (1, '{"a" : 1}'), (2, '{"a" : 2}');
SELECT count() FROM t_memory_qcc_alter WHERE dynamicType(j.a) = 'None';
ALTER TABLE t_memory_qcc_alter MODIFY COLUMN j JSON(SKIP a);
SELECT count() FROM t_memory_qcc_alter WHERE dynamicType(j.a) = 'None';
SELECT count() FROM t_memory_qcc_alter WHERE dynamicType(j.a) = 'None' SETTINGS use_query_condition_cache = 0;
DROP TABLE t_memory_qcc_alter;
