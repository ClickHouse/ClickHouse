-- Tags: no-fasttest
-- Tag no-fasttest: needs Azurite

-- An async insert flush must evaluate currentDatabase() in the database of the inserting query,
-- in column DEFAULT and MATERIALIZED expressions and in table function arguments.

DROP TABLE IF EXISTS t_async_db;

CREATE TABLE t_async_db (x UInt64, db String DEFAULT currentDatabase(), m String MATERIALIZED currentDatabase(), db_setting String DEFAULT getSetting('database')) ENGINE = MergeTree ORDER BY x;

INSERT INTO t_async_db (x) SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES (1);
INSERT INTO t_async_db (x) SETTINGS async_insert = 1, wait_for_async_insert = 1, async_insert_select_as_async_insert = 1 SELECT 2;
-- An empty `database` setting keeps the current database, and the setting stays empty.
INSERT INTO t_async_db (x) SETTINGS async_insert = 1, wait_for_async_insert = 1, database = '' VALUES (3);
INSERT INTO t_async_db (x) SETTINGS async_insert = 0 VALUES (4);

SELECT x, db = currentDatabase(), m = currentDatabase() FROM t_async_db ORDER BY x;
SELECT db_setting = '' FROM t_async_db WHERE x = 3;

INSERT INTO FUNCTION azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;', 'cont05898', concat(currentDatabase(), '_values.csv'), 'CSV', 'auto', 'x UInt64')
SETTINGS async_insert = 1, wait_for_async_insert = 1, azure_truncate_on_insert = 1 VALUES (5);

SELECT * FROM azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;', 'cont05898', concat(currentDatabase(), '_values.csv'), 'CSV', 'auto', 'x UInt64');

-- Every insert above except the `async_insert = 0` one went through the queue.
SYSTEM FLUSH LOGS query_log, asynchronous_insert_log;
SELECT count() FROM system.asynchronous_insert_log
WHERE event_date >= yesterday() AND status = 'Ok' AND query_id IN (
    SELECT query_id FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_kind = 'Insert');

DROP TABLE t_async_db;
