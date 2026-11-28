-- A read of nothing but the `Merge` virtual columns from a `Distributed` child whose substitute read is a
-- physical column named `_table` or `_database`: the references to the virtual columns are replaced by the
-- child's name, and the physical column of that name still has to be read from the shards.

SET allow_reserved_column_names = 1;

DROP TABLE IF EXISTS merge_vc_named;
DROP TABLE IF EXISTS merge_vc_named_dist;
DROP TABLE IF EXISTS merge_vc_named_child;

CREATE TABLE merge_vc_named_child (`_table` UInt16, p UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO merge_vc_named_child VALUES (7, 100), (8, 200);
CREATE TABLE merge_vc_named_dist AS merge_vc_named_child ENGINE = Distributed(test_shard_localhost, currentDatabase(), merge_vc_named_child);
CREATE TABLE merge_vc_named (p String) ENGINE = Merge(currentDatabase(), '^merge_vc_named_dist$');

SELECT _table FROM merge_vc_named SETTINGS prefer_localhost_replica = 0;
SELECT _table FROM merge_vc_named SETTINGS prefer_localhost_replica = 1;
SELECT _table AS p FROM merge_vc_named SETTINGS prefer_localhost_replica = 0;
SELECT _table, count() FROM merge_vc_named GROUP BY _table SETTINGS prefer_localhost_replica = 0;
SELECT _database = currentDatabase(), _table FROM merge_vc_named WHERE _table = 'merge_vc_named_dist' SETTINGS prefer_localhost_replica = 0;
SELECT _table, p FROM merge_vc_named ORDER BY p SETTINGS prefer_localhost_replica = 0;

DROP TABLE merge_vc_named;
DROP TABLE merge_vc_named_dist;
DROP TABLE merge_vc_named_child;

CREATE TABLE merge_vc_named_child (`_database` UInt16, p UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO merge_vc_named_child VALUES (7, 100), (8, 200);
CREATE TABLE merge_vc_named_dist AS merge_vc_named_child ENGINE = Distributed(test_shard_localhost, currentDatabase(), merge_vc_named_child);
CREATE TABLE merge_vc_named (p String) ENGINE = Merge(currentDatabase(), '^merge_vc_named_dist$');

SELECT _database = currentDatabase() FROM merge_vc_named SETTINGS prefer_localhost_replica = 0;
SELECT _database = currentDatabase() FROM merge_vc_named SETTINGS prefer_localhost_replica = 1;
SELECT _table FROM merge_vc_named SETTINGS prefer_localhost_replica = 0;
SELECT _table, _database = currentDatabase(), count() FROM merge_vc_named GROUP BY ALL SETTINGS prefer_localhost_replica = 0;

DROP TABLE merge_vc_named;
DROP TABLE merge_vc_named_dist;
DROP TABLE merge_vc_named_child;

-- The same through a `Merge` nested in another one, which declares that physical column itself.
CREATE TABLE merge_vc_named_child (`_table` UInt16, p UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO merge_vc_named_child VALUES (7, 100), (8, 200);
CREATE TABLE merge_vc_named_dist (`_table` UInt16, p UInt64) ENGINE = Merge(currentDatabase(), '^merge_vc_named_child$');
CREATE TABLE merge_vc_named (p String) ENGINE = Merge(currentDatabase(), '^merge_vc_named_dist$');

SELECT _table FROM merge_vc_named;
SELECT _table AS p FROM merge_vc_named;
SELECT _table, count() FROM merge_vc_named GROUP BY _table;
SELECT count() FROM merge_vc_named WHERE _table = 'merge_vc_named_dist';

DROP TABLE merge_vc_named;
DROP TABLE merge_vc_named_dist;
DROP TABLE merge_vc_named_child;
