-- A read of nothing but the `Merge` virtual columns from a `Distributed` child, with a column the
-- `Merge` declares under another type, must keep the substitute read in the child query.

DROP TABLE IF EXISTS merge_vc_conv;
DROP TABLE IF EXISTS merge_vc_conv_dist;
DROP TABLE IF EXISTS merge_vc_conv_child;

CREATE TABLE merge_vc_conv_child (p UInt8) ENGINE = MergeTree ORDER BY p;
INSERT INTO merge_vc_conv_child VALUES (7), (8);
CREATE TABLE merge_vc_conv_dist AS merge_vc_conv_child ENGINE = Distributed(test_shard_localhost, currentDatabase(), merge_vc_conv_child);
CREATE TABLE merge_vc_conv (p String) ENGINE = Merge(currentDatabase(), '^merge_vc_conv_dist$');

SELECT _table FROM merge_vc_conv SETTINGS prefer_localhost_replica = 0;
SELECT _table, count() FROM merge_vc_conv GROUP BY _table SETTINGS prefer_localhost_replica = 0;
SELECT _table, count() FROM merge_vc_conv GROUP BY _table HAVING count() > 1 SETTINGS prefer_localhost_replica = 0;
SELECT max(_table), count() FROM merge_vc_conv SETTINGS prefer_localhost_replica = 0;
SELECT _table, row_number() OVER () AS n FROM merge_vc_conv ORDER BY n SETTINGS prefer_localhost_replica = 0;
SELECT _database = currentDatabase(), _table FROM merge_vc_conv WHERE _table = 'merge_vc_conv_dist' LIMIT 1 SETTINGS prefer_localhost_replica = 0;
SELECT _table, p FROM merge_vc_conv ORDER BY p SETTINGS prefer_localhost_replica = 0;
SELECT _table, count() FROM merge_vc_conv GROUP BY _table SETTINGS prefer_localhost_replica = 1;

DROP TABLE merge_vc_conv;
DROP TABLE merge_vc_conv_dist;
DROP TABLE merge_vc_conv_child;
