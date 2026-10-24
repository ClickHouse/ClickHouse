-- Random settings limits: enable_multiple_prewhere_read_steps=(1, 1); optimize_move_to_prewhere=(1, 1); query_plan_merge_filters=(1, 1); query_plan_convert_outer_join_to_inner_join=(1, 1)
-- A JOIN ON condition repeated in WHERE under join_use_nulls is computed twice under one name with different
-- types, and a PREWHERE split into steps evaluates both.

CREATE TABLE t1 (id Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t2 (id Int64, reviewer Int64, enabled Bool) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t2_mem (id Int64, reviewer Int64, enabled Bool) ENGINE = Memory;
INSERT INTO t1 VALUES (1), (2);
INSERT INTO t2 VALUES (1, 100, true), (2, 200, false);
INSERT INTO t2_mem SELECT * FROM t2;

SET join_use_nulls = 1, allow_reorder_prewhere_conditions = 0;

SELECT t1.id, t2.reviewer FROM t1 LEFT JOIN t2 ON t1.id = t2.id AND t2.enabled = true AND t2.reviewer > 0
WHERE t2.enabled = true AND t2.reviewer = 100 ORDER BY t1.id;
SELECT t1.id, t2.reviewer FROM t1 LEFT JOIN t2 ON t1.id = t2.id AND t2.reviewer > 0 AND t2.enabled = true
WHERE t2.enabled = true AND t2.reviewer = 100 ORDER BY t1.id;
SELECT t1.id, t2.reviewer FROM t1 LEFT JOIN t2_mem AS t2 ON t1.id = t2.id AND t2.enabled = true AND t2.reviewer > 0
WHERE t2.enabled = true AND t2.reviewer = 100 ORDER BY t1.id;

-- Two conditions on one column: a step evaluates both, and its filter is named after them.
SELECT t1.id, t2.reviewer FROM t1 LEFT JOIN t2 ON t1.id = t2.id AND t2.reviewer > 50 AND t2.reviewer < 150 AND t2.id > 0
WHERE t2.reviewer > 50 AND t2.reviewer < 150 AND t2.enabled = true ORDER BY t1.id;

-- The steps still filter the rows a later default expression is evaluated on.
CREATE TABLE t3 (id Int64, enabled Bool, v String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t3 VALUES (1, true, '42'), (2, false, 'invalid');
ALTER TABLE t3 ADD COLUMN n UInt64 DEFAULT toUInt64(v);
SELECT t1.id, t3.n FROM t1 LEFT JOIN t3 ON t1.id = t3.id AND t3.enabled = true AND t3.id > 0
WHERE t3.enabled = true AND t3.n > 0 ORDER BY t1.id;

-- A new name is not taken from a stored column.
CREATE TABLE t4 (id Int64, reviewer Int64, enabled Bool, `__prewhere_equals(__table2.enabled, 1_Bool)` UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t4_mem AS t4 ENGINE = Memory;
INSERT INTO t4 VALUES (1, 100, true, 7), (2, 200, false, 8);
INSERT INTO t4_mem SELECT * FROM t4;
SELECT t1.id, t4.reviewer, t4.`__prewhere_equals(__table2.enabled, 1_Bool)` FROM t1 LEFT JOIN t4 ON t1.id = t4.id AND t4.enabled = true AND t4.reviewer > 0
WHERE t4.enabled = true AND t4.reviewer = 100 ORDER BY t1.id;
SELECT t1.id, t4.reviewer, t4.`__prewhere_equals(__table2.enabled, 1_Bool)` FROM t1 LEFT JOIN t4_mem AS t4 ON t1.id = t4.id AND t4.enabled = true AND t4.reviewer > 0
WHERE t4.enabled = true AND t4.reviewer = 100 ORDER BY t1.id;

-- The same for the filter of a step with two conditions.
CREATE TABLE t5 (id Int64, reviewer Int64, enabled Bool, `and(__prewhere_greater(__table2.reviewer, 50_UInt8), __prewhere_less(__table2.reviewer, 150_UInt8))` UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t5_mem AS t5 ENGINE = Memory;
INSERT INTO t5 VALUES (1, 100, true, 7), (2, 200, false, 8);
INSERT INTO t5_mem SELECT * FROM t5;
SELECT t1.id, t5.reviewer, t5.`and(__prewhere_greater(__table2.reviewer, 50_UInt8), __prewhere_less(__table2.reviewer, 150_UInt8))` FROM t1 LEFT JOIN t5 ON t1.id = t5.id AND t5.reviewer > 50 AND t5.reviewer < 150 AND t5.id > 0
WHERE t5.reviewer > 50 AND t5.reviewer < 150 AND t5.enabled = true ORDER BY t1.id;
SELECT t1.id, t5.reviewer, t5.`and(__prewhere_greater(__table2.reviewer, 50_UInt8), __prewhere_less(__table2.reviewer, 150_UInt8))` FROM t1 LEFT JOIN t5_mem AS t5 ON t1.id = t5.id AND t5.reviewer > 50 AND t5.reviewer < 150 AND t5.id > 0
WHERE t5.reviewer > 50 AND t5.reviewer < 150 AND t5.enabled = true ORDER BY t1.id;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t2_mem;
DROP TABLE t3;
DROP TABLE t4;
DROP TABLE t4_mem;
DROP TABLE t5;
DROP TABLE t5_mem;
