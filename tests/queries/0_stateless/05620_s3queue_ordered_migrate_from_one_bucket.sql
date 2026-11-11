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

DROP TABLE q_one;
DROP TABLE q_zero;
DROP TABLE q_empty;
DROP TABLE q_reset;
