-- Tags: no-fasttest, zookeeper, no-shared-catalog
-- no-fasttest: the `S3Queue` table engine is not built in the fast test.
-- zookeeper: the migration to buckets is done in Keeper.
-- no-shared-catalog: with a shared catalog each `ALTER TABLE ... MODIFY SETTING` becomes a distributed DDL.

-- `ALTER TABLE ... MODIFY SETTING buckets` of one table migrates the queue in Keeper. Another table on the same
-- `keeper_path` keeps one bucket in its definition and is loaded with the buckets from Keeper.

-- A replayed CREATE would be another queue on the same `keeper_path`.
SET ast_fuzzer_any_query = 0;
SET s3queue_migrate_old_metadata_to_buckets = 1;

CREATE TABLE q (x UInt64)
ENGINE = S3Queue('http://localhost:11111/test/05816_q/', 'username', 'password', CSV)
SETTINGS mode = 'ordered', buckets = 1, last_processed_path = 'a.csv', keeper_path = '/clickhouse/{database}/q';
CREATE TABLE peer (x UInt64)
ENGINE = S3Queue('http://localhost:11111/test/05816_q/', 'username', 'password', CSV)
SETTINGS mode = 'ordered', buckets = 1, last_processed_path = 'a.csv', keeper_path = '/clickhouse/{database}/q';

ALTER TABLE peer MODIFY SETTING buckets = 2;

DETACH TABLE q;
DETACH TABLE peer;
-- Loads alone, so its metadata is built from its own definition and Keeper.
ATTACH TABLE q;
SELECT 'attached', value FROM system.s3_queue_settings
WHERE database = currentDatabase() AND table = 'q' AND name = 'buckets';

-- Identical to `q` except `buckets`: a definition that already uses buckets must match Keeper.
CREATE TABLE q_three (x UInt64)
ENGINE = S3Queue('http://localhost:11111/test/05816_q/', 'username', 'password', CSV)
SETTINGS mode = 'ordered', buckets = 3, last_processed_path = 'a.csv', keeper_path = '/clickhouse/{database}/q'; -- { serverError METADATA_MISMATCH }

ALTER TABLE q MODIFY SETTING buckets = 2;
SELECT 'persisted', extract(create_table_query, 'buckets = (\\d+)') FROM system.tables
WHERE database = currentDatabase() AND name = 'q';

ATTACH TABLE peer;
DROP TABLE peer;
DROP TABLE q;
