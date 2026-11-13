-- Tags: no-fasttest, zookeeper, no-shared-catalog
-- no-fasttest: the `S3Queue` table engine is not built in the fast test.
-- zookeeper: the migration moves the `processed` nodes of the table inside Keeper.
-- no-shared-catalog: with a shared catalog each `ALTER TABLE ... MODIFY SETTING` becomes a distributed DDL
-- that can outlast `distributed_ddl_task_timeout` under load; this test targets the Keeper layout only.

-- An Ordered queue with at most one bucket keeps its last processed file in `<keeper_path>/processed`, a queue
-- with more buckets keeps it in every `buckets/<bucket>/processed`. After `ALTER TABLE ... MODIFY SETTING buckets`
-- the layout must be the one of the new number of buckets, which is `processing_threads_num` for `buckets = 0`.
-- Only Keeper is touched: `last_processed_path` makes CREATE TABLE write the pointer.

-- A replayed CREATE would be a second queue on the same `keeper_path`.
SET ast_fuzzer_any_query = 0;
SET s3queue_migrate_old_metadata_to_buckets = 1;

-- 1. From one bucket to two.
CREATE TABLE q_one (x UInt64)
ENGINE = S3Queue('http://localhost:11111/test/05620_one/', 'username', 'password', CSV)
SETTINGS mode = 'ordered', buckets = 1, last_processed_path = 'one.csv',
         keeper_path = '/clickhouse/{database}/q_one';

ALTER TABLE q_one MODIFY SETTING buckets = 2;

SELECT 'one / processed', count() FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_one' AND name = 'processed';
SELECT 'one / bucket 0', count(), max(JSONExtractString(value, 'file_path')) FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_one/buckets/0' AND name = 'processed';
SELECT 'one / bucket 1', count(), max(JSONExtractString(value, 'file_path')) FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_one/buckets/1' AND name = 'processed';

-- 2. To one bucket the layout stays, then from it to two buckets.
CREATE TABLE q_zero (x UInt64)
ENGINE = S3Queue('http://localhost:11111/test/05620_zero/', 'username', 'password', CSV)
SETTINGS mode = 'ordered', processing_threads_num = 1, last_processed_path = 'zero.csv',
         keeper_path = '/clickhouse/{database}/q_zero';

ALTER TABLE q_zero MODIFY SETTING buckets = 1;

SELECT 'zero / processed', count(), max(JSONExtractString(value, 'file_path')) FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_zero' AND name = 'processed';
SELECT 'zero / buckets', count() FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_zero' AND name = 'buckets';

ALTER TABLE q_zero MODIFY SETTING buckets = 2;

SELECT 'zero / processed after', count() FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_zero' AND name = 'processed';
SELECT 'zero / bucket 0 after', count(), max(JSONExtractString(value, 'file_path')) FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_zero/buckets/0' AND name = 'processed';
SELECT 'zero / bucket 1 after', count(), max(JSONExtractString(value, 'file_path')) FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_zero/buckets/1' AND name = 'processed';

-- 3. Nothing processed yet: the bucket nodes are still created.
CREATE TABLE q_empty (x UInt64)
ENGINE = S3Queue('http://localhost:11111/test/05620_empty/', 'username', 'password', CSV)
SETTINGS mode = 'ordered', processing_threads_num = 1,
         keeper_path = '/clickhouse/{database}/q_empty';

ALTER TABLE q_empty MODIFY SETTING buckets = 2;

SELECT 'empty / buckets', count() FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_empty/buckets';

-- 4. `buckets = 0` means `processing_threads_num` buckets.
CREATE TABLE q_reset (x UInt64)
ENGINE = S3Queue('http://localhost:11111/test/05620_reset/', 'username', 'password', CSV)
SETTINGS mode = 'ordered', buckets = 1, processing_threads_num = 2, last_processed_path = 'two.csv',
         keeper_path = '/clickhouse/{database}/q_reset';

ALTER TABLE q_reset MODIFY SETTING buckets = 0;

SELECT 'reset / processed', count() FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_reset' AND name = 'processed';
SELECT 'reset / bucket 0', count(), max(JSONExtractString(value, 'file_path')) FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_reset/buckets/0' AND name = 'processed';
SELECT 'reset / bucket 1', count(), max(JSONExtractString(value, 'file_path')) FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_reset/buckets/1' AND name = 'processed';

-- 5. A pointer left in bucket 0 of a `buckets = 1` queue is migrated while no other bucket exists.
CREATE TABLE q_legacy (x UInt64)
ENGINE = S3Queue('http://localhost:11111/test/05620_legacy/', 'username', 'password', CSV)
SETTINGS mode = 'ordered', buckets = 1,
         keeper_path = '/clickhouse/{database}/q_legacy';

INSERT INTO system.zookeeper (path, name, value)
SELECT '/clickhouse/' || currentDatabase() || '/q_legacy/buckets/0', 'processed',
       '{"file_path":"legacy.csv","last_processed_timestamp":0,"last_exception":"","retries":0,"processor_id":""}';

ALTER TABLE q_legacy MODIFY SETTING buckets = 2;

SELECT 'legacy / processed', count() FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_legacy' AND name = 'processed';
SELECT 'legacy / bucket 0', count(), max(JSONExtractString(value, 'file_path')) FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_legacy/buckets/0' AND name = 'processed';
SELECT 'legacy / bucket 1', count(), max(JSONExtractString(value, 'file_path')) FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_legacy/buckets/1' AND name = 'processed';

-- 6. With pointers in both `<keeper_path>/processed` and bucket 0, the greater one is migrated.
CREATE TABLE q_dual_root (x UInt64)
ENGINE = S3Queue('http://localhost:11111/test/05620_dual_root/', 'username', 'password', CSV)
SETTINGS mode = 'ordered', buckets = 1, last_processed_path = 'd.csv',
         keeper_path = '/clickhouse/{database}/q_dual_root';

INSERT INTO system.zookeeper (path, name, value)
SELECT '/clickhouse/' || currentDatabase() || '/q_dual_root/buckets/0', 'processed',
       '{"file_path":"c.csv","last_processed_timestamp":0,"last_exception":"","retries":0,"processor_id":""}';

ALTER TABLE q_dual_root MODIFY SETTING buckets = 2;

SELECT 'dual root / processed', count() FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_dual_root' AND name = 'processed';
SELECT 'dual root / bucket 0', count(), max(JSONExtractString(value, 'file_path')) FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_dual_root/buckets/0' AND name = 'processed';
SELECT 'dual root / bucket 1', count(), max(JSONExtractString(value, 'file_path')) FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_dual_root/buckets/1' AND name = 'processed';

CREATE TABLE q_dual_bucket (x UInt64)
ENGINE = S3Queue('http://localhost:11111/test/05620_dual_bucket/', 'username', 'password', CSV)
SETTINGS mode = 'ordered', buckets = 1, last_processed_path = 'a.csv',
         keeper_path = '/clickhouse/{database}/q_dual_bucket';

INSERT INTO system.zookeeper (path, name, value)
SELECT '/clickhouse/' || currentDatabase() || '/q_dual_bucket/buckets/0', 'processed',
       '{"file_path":"b.csv","last_processed_timestamp":0,"last_exception":"","retries":0,"processor_id":""}';

ALTER TABLE q_dual_bucket MODIFY SETTING buckets = 2;

SELECT 'dual bucket / processed', count() FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_dual_bucket' AND name = 'processed';
SELECT 'dual bucket / bucket 0', count(), max(JSONExtractString(value, 'file_path')) FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_dual_bucket/buckets/0' AND name = 'processed';
SELECT 'dual bucket / bucket 1', count(), max(JSONExtractString(value, 'file_path')) FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_dual_bucket/buckets/1' AND name = 'processed';

-- 7. Bucket 0's pointer is the queue's until the table metadata in Keeper has more buckets:
-- `buckets/1` left by an interrupted migration does not change it, a migration by another server does.
CREATE TABLE q_interrupted (x UInt64)
ENGINE = S3Queue('http://localhost:11111/test/05620_interrupted/', 'username', 'password', CSV)
SETTINGS mode = 'ordered', buckets = 1,
         keeper_path = '/clickhouse/{database}/q_interrupted';

INSERT INTO system.zookeeper (path, name, value)
SELECT '/clickhouse/' || currentDatabase() || '/q_interrupted/buckets', '1', '';
INSERT INTO system.zookeeper (path, name, value)
SELECT '/clickhouse/' || currentDatabase() || '/q_interrupted/buckets/0', 'processed',
       '{"file_path":"legacy.csv","last_processed_timestamp":0,"last_exception":"","retries":0,"processor_id":""}';

ALTER TABLE q_interrupted MODIFY SETTING buckets = 2;

SELECT 'interrupted / processed', count() FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_interrupted' AND name = 'processed';
SELECT 'interrupted / bucket 0', count(), max(JSONExtractString(value, 'file_path')) FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_interrupted/buckets/0' AND name = 'processed';
SELECT 'interrupted / bucket 1', count(), max(JSONExtractString(value, 'file_path')) FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_interrupted/buckets/1' AND name = 'processed';

CREATE TABLE q_peer (x UInt64)
ENGINE = S3Queue('http://localhost:11111/test/05620_peer/', 'username', 'password', CSV)
SETTINGS mode = 'ordered', buckets = 1,
         keeper_path = '/clickhouse/{database}/q_peer';

INSERT INTO system.zookeeper (path, name, value)
SELECT '/clickhouse/' || currentDatabase() || '/q_peer/buckets', '1', '';
INSERT INTO system.zookeeper (path, name, value)
SELECT '/clickhouse/' || currentDatabase() || '/q_peer/buckets/0', 'processed',
       '{"file_path":"live.csv","last_processed_timestamp":0,"last_exception":"","retries":0,"processor_id":""}';
INSERT INTO system.zookeeper (path, name, value)
SELECT path, name, replaceOne(value, '"buckets":1', '"buckets":2') FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_peer' AND name = 'metadata';

ALTER TABLE q_peer MODIFY SETTING buckets = 2;

SELECT 'peer / processed', count() FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_peer' AND name = 'processed';
SELECT 'peer / bucket 0', count(), max(JSONExtractString(value, 'file_path')) FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_peer/buckets/0' AND name = 'processed';
SELECT 'peer / bucket 1', count(), max(JSONExtractString(value, 'file_path')) FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_peer/buckets/1' AND name = 'processed';

-- 8. A refused migration creates no bucket nodes.
CREATE TABLE q_part (x UInt64)
ENGINE = S3Queue('http://localhost:11111/test/05620_part/', 'username', 'password', CSV)
SETTINGS mode = 'ordered', buckets = 1, last_processed_path = 'd.csv',
         keeper_path = '/clickhouse/{database}/q_part';

INSERT INTO system.zookeeper (path, name, value)
SELECT '/clickhouse/' || currentDatabase() || '/q_part/buckets/0', 'processed',
       '{"file_path":"c.csv","last_processed_timestamp":0,"last_exception":"","retries":0,"processor_id":""}';

-- The child under `<keeper_path>/processed` has the shape of a partitioned queue's pointers.
CREATE TABLE q_part_holder (x UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/q_part/processed/holder', '{replica}')
ORDER BY tuple();

ALTER TABLE q_part MODIFY SETTING buckets = 2; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'part / bucket 1 after refusal', count() FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_part/buckets' AND name = '1';
SELECT 'part / bucket 0 after refusal', count(), max(JSONExtractString(value, 'file_path')) FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/q_part/buckets/0' AND name = 'processed';

DROP TABLE q_one;
DROP TABLE q_zero;
DROP TABLE q_empty;
DROP TABLE q_reset;
DROP TABLE q_legacy;
DROP TABLE q_dual_root;
DROP TABLE q_dual_bucket;
DROP TABLE q_interrupted;
DROP TABLE q_peer;
DROP TABLE q_part_holder SYNC;
DROP TABLE q_part;
