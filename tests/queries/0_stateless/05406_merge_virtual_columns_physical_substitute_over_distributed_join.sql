-- A query over nothing but the `Merge` virtual columns reads a substitute column of each child. With a
-- `JOIN` or `ARRAY JOIN` over a `Distributed` child whose substitute is a physical `_table` or `_database`,
-- the substitute must stay the child's column, not be replaced by the `Merge` virtual column constant.

DROP TABLE IF EXISTS m;
DROP TABLE IF EXISTS dist_t;
DROP TABLE IF EXISTS dist_d;
DROP TABLE IF EXISTS child_t;
DROP TABLE IF EXISTS child_d;

CREATE TABLE child_t (`_table` UInt8) ENGINE = MergeTree ORDER BY `_table`;
CREATE TABLE child_d (`_database` UInt8) ENGINE = MergeTree ORDER BY `_database`;
INSERT INTO child_t VALUES (7), (8);
INSERT INTO child_d VALUES (9);

CREATE TABLE dist_t AS child_t ENGINE = Distributed(test_shard_localhost, currentDatabase(), child_t);
CREATE TABLE dist_d AS child_d ENGINE = Distributed(test_shard_localhost, currentDatabase(), child_d);

CREATE TABLE m (x UInt8) ENGINE = Merge(currentDatabase(), '^dist_');

SET prefer_localhost_replica = 0;

SELECT _table FROM m ARRAY JOIN [1] AS one ORDER BY _table;
SELECT _database = currentDatabase(), _table, one FROM m ARRAY JOIN [1, 2] AS one ORDER BY _table, one;
SELECT _table, n FROM m JOIN (SELECT 1 AS n) AS t ON 1 ORDER BY _table;
SELECT _table, count() FROM m ARRAY JOIN [1, 2] AS one GROUP BY _table ORDER BY _table;

DROP TABLE m;
DROP TABLE dist_t;
DROP TABLE dist_d;
DROP TABLE child_t;
DROP TABLE child_d;
