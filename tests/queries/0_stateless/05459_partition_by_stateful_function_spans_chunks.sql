-- Tags: no-fasttest
-- Tag no-fasttest: uses S3

SET max_block_size = 1000, max_insert_threads = 1, engine_file_truncate_on_insert = 1;

-- file(): the counter spans the five 1000-row chunks
INSERT INTO FUNCTION file(currentDatabase() || '/05459_rn_{_partition_id}.csv', 'CSV', 'n UInt64')
PARTITION BY intDiv(rowNumberInAllBlocks(), 1000)
SELECT number FROM numbers(5000);
SELECT _file, count(), min(n), max(n) FROM file(currentDatabase() || '/05459_rn_*.csv', 'CSV', 'n UInt64') GROUP BY _file ORDER BY _file;

-- S3 engine (strategy shared by all INSERTs into the table): spans chunks, starts anew with the next INSERT
CREATE TABLE t_05459 (n UInt64)
ENGINE = S3(s3_conn, filename = currentDatabase() || '/05459_eng_{_partition_id}.csv', format = CSV, partition_strategy = 'wildcard')
PARTITION BY intDiv(rowNumberInAllBlocks(), 1000);
INSERT INTO t_05459 SELECT number FROM numbers(5000) SETTINGS s3_truncate_on_insert = 1, s3_create_new_file_on_insert = 0;
INSERT INTO t_05459 SELECT number + 10000 FROM numbers(2000) SETTINGS s3_truncate_on_insert = 1, s3_create_new_file_on_insert = 0;
SELECT _file, count(), min(n), max(n) FROM s3(s3_conn, filename = currentDatabase() || '/05459_eng_*.csv', format = CSV, structure = 'n UInt64') GROUP BY _file ORDER BY _file;
DROP TABLE t_05459;

-- now64() is evaluated once per INSERT, not once per chunk
INSERT INTO FUNCTION file(currentDatabase() || '/05459_now_{_partition_id}.csv', 'CSV', 'n UInt64')
PARTITION BY toUnixTimestamp64Nano(now64(9))
SELECT number FROM numbers(5000);
SELECT count(DISTINCT _file), count() FROM file(currentDatabase() || '/05459_now_*.csv', 'CSV', 'n UInt64');
