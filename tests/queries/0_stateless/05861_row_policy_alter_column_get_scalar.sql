-- a scalar subquery of an Array, Tuple or LowCardinality type is also a scalar subquery for the modify column check

-- a remote parallel replica analyzes a policy in its own current database, which does not have the lookup tables
SET enable_parallel_replicas = 0;

CREATE TABLE t (id UInt32, n UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE u (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t VALUES (1, 1), (2, 2);
INSERT INTO u VALUES (1);
-- the analysis gives the subquery an empty array, so `n = 1` is not evaluated
CREATE ROW POLICY p_05861 ON t FOR SELECT USING notEmpty((SELECT groupArray(id) FROM u)) AND n = 1 TO CURRENT_USER;

-- a read evaluates `n = 1`, which has no common type for a String n
ALTER TABLE t MODIFY COLUMN n String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'refused', id, toTypeName(n) FROM t;

DROP ROW POLICY p_05861 ON t;
CREATE ROW POLICY p_05861 ON t FOR SELECT USING if(notEmpty((SELECT groupArray(id) FROM u)), n = 1, 0) TO CURRENT_USER;
ALTER TABLE t MODIFY COLUMN n String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'refused', id, toTypeName(n) FROM t;

-- the same type
ALTER TABLE t MODIFY COLUMN n UInt32 DEFAULT 1;
SELECT 'kept', id FROM t;

DROP ROW POLICY p_05861 ON t;
DROP TABLE t;
DROP TABLE u;
