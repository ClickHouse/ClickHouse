-- An `ALIAS` column of a child whose expression reads no column of the child (`y ALIAS 1`) needs a
-- substitute read, as a column the child does not have. With a `JOIN` or `ARRAY JOIN` over a
-- `Distributed` child, the substitute must be part of the child query sent to the shards.

DROP TABLE IF EXISTS m_nested;
DROP TABLE IF EXISTS m;
DROP TABLE IF EXISTS dist;
DROP TABLE IF EXISTS child;

CREATE TABLE child (p UInt8, y UInt8 ALIAS 1) ENGINE = MergeTree ORDER BY p;
INSERT INTO child (p) VALUES (7), (8);

CREATE TABLE dist AS child ENGINE = Distributed(test_shard_localhost, currentDatabase(), child);
CREATE TABLE m (y UInt8) ENGINE = Merge(currentDatabase(), '^dist$');
CREATE TABLE m_nested (y UInt8) ENGINE = Merge(currentDatabase(), '^m$');

SET prefer_localhost_replica = 0;

SELECT _table, y FROM m ARRAY JOIN [1] AS one;
SELECT _database = currentDatabase(), y FROM m ARRAY JOIN [1] AS one;
SELECT _table, y, n FROM m JOIN (SELECT 1 AS n) AS t ON m.y = t.n;
SELECT _table, y FROM m_nested ARRAY JOIN [1] AS one;
SELECT _table, y FROM m;

DROP TABLE m_nested;
DROP TABLE m;
DROP TABLE dist;
DROP TABLE child;
