-- A read limit of the `RETURNING` query was set on every processor of the inserted data, which are the processors of the insert,
-- so in the `break` mode it cancelled the source of the insert, and the query reported success with a part of the rows:
-- https://github.com/ClickHouse/ClickHouse/issues/127724

DROP TABLE IF EXISTS t_returning_read_limit;
CREATE TABLE t_returning_read_limit (x UInt64) ENGINE = MergeTree ORDER BY x;

INSERT INTO t_returning_read_limit
RETURNING (SELECT count() > 0 FROM t_returning_read_limit SETTINGS max_rows_to_read = 20, read_overflow_mode = 'break')
SELECT number FROM numbers(100) SETTINGS max_block_size = 10;

SELECT count() FROM t_returning_read_limit;

DROP TABLE t_returning_read_limit;
