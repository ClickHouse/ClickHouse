-- a modify column must not give a column a type for which a read fails to evaluate the row policy, also when analysis accepts it

CREATE TABLE t (id UInt32, n UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE u (id UInt32, f UInt8) ENGINE = MergeTree ORDER BY id;
CREATE TABLE m (k UInt32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE a ENGINE = Alias(m);
INSERT INTO t VALUES (1, 1), (2, 2);
INSERT INTO u VALUES (1, 1), (2, 0);
INSERT INTO m VALUES (1);
CREATE ROW POLICY p_05858 ON t FOR SELECT USING n = 1 TO CURRENT_USER;
CREATE ROW POLICY p_05858 ON u FOR SELECT USING f TO CURRENT_USER;
CREATE ROW POLICY p_05858 ON a FOR SELECT USING 1 IN (SELECT k FROM m) TO CURRENT_USER;

-- `n = 1` has no common type for a String n
ALTER TABLE t MODIFY COLUMN n String; -- { serverError NO_COMMON_TYPE }
-- a String filter
ALTER TABLE u MODIFY COLUMN f String; -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }
SELECT 'refused', id FROM t;
SELECT 'refused', id FROM u;

-- types for which the policies still evaluate
ALTER TABLE t MODIFY COLUMN n UInt64 SETTINGS mutations_sync = 2;
ALTER TABLE u MODIFY COLUMN f Nullable(UInt8) SETTINGS mutations_sync = 2;
SELECT 'kept', id FROM t;
SELECT 'kept', id FROM u;

-- an alias policy that reads the table only in a subquery
ALTER TABLE m MODIFY COLUMN k UInt32 DEFAULT 1;
SELECT 'subquery', k FROM a;

DROP ROW POLICY p_05858 ON t;
DROP ROW POLICY p_05858 ON u;
DROP ROW POLICY p_05858 ON a;
DROP TABLE a;
DROP TABLE m;
DROP TABLE t;
DROP TABLE u;
