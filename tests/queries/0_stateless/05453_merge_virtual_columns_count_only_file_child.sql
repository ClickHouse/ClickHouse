-- Tags: no-fasttest
-- Tag no-fasttest: depends on Parquet

-- A query over nothing but the `Merge` virtual columns reads no column of a file-like child, so the
-- child counts the rows from the metadata of its files instead of reading their data.

DROP TABLE IF EXISTS m;
DROP TABLE IF EXISTS child;

CREATE TABLE child (x String) ENGINE = File(Parquet);
INSERT INTO child SELECT repeat(toString(cityHash64(number)), 50) FROM numbers(100000);

CREATE TABLE m (x String) ENGINE = Merge(currentDatabase(), '^child$');

SET optimize_count_from_files = 1;

SELECT _table, count() FROM m GROUP BY _table SETTINGS log_comment = '05453_table';
SELECT _database = currentDatabase(), count() FROM m GROUP BY _database SETTINGS log_comment = '05453_database';
SELECT count() FROM m WHERE _table = 'child' SETTINGS log_comment = '05453_filter';
SELECT _table, count(x) FROM m GROUP BY _table SETTINGS log_comment = '05453_column';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, read_bytes > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05453_table', '05453_database', '05453_filter', '05453_column')
ORDER BY event_time_microseconds;

DROP TABLE m;
DROP TABLE child;
