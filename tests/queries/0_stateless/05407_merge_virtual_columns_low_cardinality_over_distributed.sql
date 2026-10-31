-- The `_database` and `_table` virtual columns of a `Merge` table keep their `LowCardinality(String)` type
-- in a child that executes the query itself (a `Distributed` child above `FetchColumns`), directly and
-- through a nested `Merge`.

DROP TABLE IF EXISTS m_outer;
DROP TABLE IF EXISTS m;
DROP TABLE IF EXISTS dist;
DROP TABLE IF EXISTS leaf;

CREATE TABLE leaf (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO leaf VALUES (1);
CREATE TABLE dist AS leaf ENGINE = Distributed(test_shard_localhost, currentDatabase(), leaf);
CREATE TABLE m (x UInt8) ENGINE = Merge(currentDatabase(), '^dist$');
CREATE TABLE m_outer (x UInt8) ENGINE = Merge(currentDatabase(), '^m$');

SET prefer_localhost_replica = 0;

SELECT lowCardinalityIndices(_table), lowCardinalityIndices(_database), toTypeName(_table), toTypeName(_database), _table FROM m;
SELECT lowCardinalityIndices(_table), lowCardinalityIndices(_database), toTypeName(_table), toTypeName(_database), _table FROM m_outer;
SELECT _table, count() FROM m WHERE lowCardinalityIndices(_table) = 1 AND lowCardinalityIndices(_database) = 1 GROUP BY _table;
SELECT _table, x FROM m_outer WHERE lowCardinalityIndices(_table) = 1;

DROP TABLE m_outer;
DROP TABLE m;
DROP TABLE dist;
DROP TABLE leaf;
