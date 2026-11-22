-- a modify column of the base column of an alias column changes what a row policy that reads the alias computes

CREATE TABLE t (id UInt32, n UInt32, a UInt8 ALIAS n = 1) ENGINE = MergeTree ORDER BY id;
CREATE TABLE s (id UInt32, n UInt32, a UInt8 ALIAS n = 1) ENGINE = MergeTree ORDER BY id;
CREATE TABLE q (id UInt32, n UInt32, a UInt8 ALIAS n = 1) ENGINE = MergeTree ORDER BY id;
CREATE TABLE d (id UInt32, n UInt32, a UInt32 ALIAS intDiv(n, 2)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE al ENGINE = Alias(q);
INSERT INTO t VALUES (1, 1), (2, 2);
INSERT INTO s VALUES (1, 1), (2, 2);
INSERT INTO q VALUES (1, 1), (2, 2);
INSERT INTO d VALUES (1, 2), (2, 0);
CREATE ROW POLICY p_05862 ON t FOR SELECT USING a = 1 TO CURRENT_USER;
CREATE ROW POLICY p_05862 ON s FOR SELECT USING a = 1 AND (SELECT 1) = 1 TO CURRENT_USER;
CREATE ROW POLICY p_05862 ON al FOR SELECT USING id IN (SELECT id FROM q WHERE a = 1) TO CURRENT_USER;
CREATE ROW POLICY p_05862 ON d FOR SELECT USING a > 0 TO CURRENT_USER;

-- `n = 1` in the alias has no common type for a String n
ALTER TABLE t MODIFY COLUMN n String; -- { serverError NO_COMMON_TYPE }
SELECT 'refused', id, toTypeName(n) FROM t;
-- the check does not evaluate a filter that reads a scalar subquery
ALTER TABLE s MODIFY COLUMN n String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'refused', id, toTypeName(n) FROM s;
-- a subquery reads the table before the alter
ALTER TABLE q MODIFY COLUMN n String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'refused', id, toTypeName(n) FROM al;
-- the alter itself refuses an alias expression that does not resolve
ALTER TABLE d MODIFY COLUMN n String; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT 'refused', id, toTypeName(n) FROM d;

-- a type for which the alias still evaluates
ALTER TABLE t MODIFY COLUMN n UInt64 SETTINGS mutations_sync = 2;
SELECT 'kept', id, toTypeName(n) FROM t;

DROP ROW POLICY p_05862 ON t;
DROP ROW POLICY p_05862 ON s;
DROP ROW POLICY p_05862 ON al;
DROP ROW POLICY p_05862 ON d;
DROP TABLE al;
DROP TABLE t;
DROP TABLE s;
DROP TABLE q;
DROP TABLE d;
