-- the check does not evaluate a filter that reads a scalar subquery, so a modify column must keep the types of the columns it reads

-- a remote parallel replica analyzes a policy in its own current database, which does not have the lookup tables
SET enable_parallel_replicas = 0;

CREATE TABLE t (id UInt32, n UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE l (k UInt32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE u (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t VALUES (1, 1), (2, 2);
INSERT INTO l VALUES (1);
INSERT INTO u VALUES (1);
CREATE ROW POLICY p_05859 ON t FOR SELECT USING n = 1 AND (SELECT 1) = 1 TO CURRENT_USER;
CREATE ROW POLICY p_05859 ON l FOR SELECT USING intDiv(k, (SELECT max(id) FROM u)) >= 0 TO CURRENT_USER;

-- `n = 1` has no common type for a String n
ALTER TABLE t MODIFY COLUMN n String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'refused', id, toTypeName(n) FROM t;
-- also a type for which the policy still evaluates
ALTER TABLE l MODIFY COLUMN k UInt64; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'refused', k, toTypeName(k) FROM l;

-- the same type, whose default value 0 for the scalar subquery the division would refuse
ALTER TABLE t MODIFY COLUMN n UInt32 DEFAULT 1;
ALTER TABLE l MODIFY COLUMN k UInt32 DEFAULT 1;
SELECT 'kept', id FROM t;
SELECT 'kept', k FROM l;

DROP ROW POLICY p_05859 ON t;
DROP ROW POLICY p_05859 ON l;
DROP TABLE t;
DROP TABLE l;
DROP TABLE u;
