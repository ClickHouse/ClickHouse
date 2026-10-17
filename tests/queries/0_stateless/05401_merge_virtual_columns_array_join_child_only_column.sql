-- A query over nothing but the `Merge` virtual columns reads a substitute column of each child.
-- With a `JOIN` or `ARRAY JOIN` that column must be resolved in the child, not in the `Merge`,
-- which may not declare it.

DROP TABLE IF EXISTS m;
DROP TABLE IF EXISTS child_y;
DROP TABLE IF EXISTS child_z;

CREATE TABLE child_y (y UInt8) ENGINE = MergeTree ORDER BY y;
CREATE TABLE child_z (x UInt8, z String) ENGINE = MergeTree ORDER BY x;
INSERT INTO child_y VALUES (7);
INSERT INTO child_z VALUES (1, 'a');

CREATE TABLE m (x UInt8) ENGINE = Merge(currentDatabase(), '^child_');

SELECT _table FROM m ARRAY JOIN [1] AS one ORDER BY _table;
SELECT _database = currentDatabase(), _table, one FROM m ARRAY JOIN [1, 2] AS one ORDER BY _table, one;
SELECT _table, n FROM m JOIN (SELECT 1 AS n) AS t ON 1 ORDER BY _table;
SELECT _table, count() FROM m ARRAY JOIN [1, 2] AS one GROUP BY _table ORDER BY _table;

DROP TABLE m;
DROP TABLE child_y;
DROP TABLE child_z;
