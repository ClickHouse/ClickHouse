-- Tags: shard
-- The sharding key of a `Distributed` table is analyzed again after `ALTER TABLE ... MODIFY COLUMN`.

-- A value that does not fit the old type is not dropped from the `IN` list sent to each shard.
CREATE TABLE loc_in (c0 Int16) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE dist_in (c0 Int16) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), loc_in, c0);
ALTER TABLE loc_in MODIFY COLUMN c0 Int32;
ALTER TABLE dist_in MODIFY COLUMN c0 Int32;
INSERT INTO loc_in VALUES (1), (2), (100000);
SELECT c0 FROM dist_in WHERE c0 IN (1, 2, 100000) ORDER BY c0 SETTINGS optimize_skip_unused_shards = 1, optimize_skip_unused_shards_rewrite_in = 1;

-- The key's result type changes with the column type.
CREATE TABLE loc_ins (c0 Int16) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE dist_ins (c0 Int16) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), loc_ins, multiply(1001, c0));
ALTER TABLE loc_ins MODIFY COLUMN c0 Int32;
ALTER TABLE dist_ins MODIFY COLUMN c0 Int32;
INSERT INTO dist_ins SETTINGS distributed_foreground_insert = 1 VALUES (4), (5);
INSERT INTO dist_ins SETTINGS distributed_foreground_insert = 0 VALUES (6);
SYSTEM FLUSH DISTRIBUTED dist_ins;
SELECT c0 FROM loc_ins ORDER BY c0;
