-- Tags: no-fasttest
-- Tag no-fasttest: needs the server's own MySQL wire port, which fasttest does not serve.

-- EXPLAIN TABLE OVERRIDE prints the summary of what a TABLE OVERRIDE changes:
-- the modified columns, the added ALIAS columns and the columns every key clause uses.
-- Expected behaviour: docs/reference/statements/explain.mdx, section "EXPLAIN TABLE OVERRIDE".
-- Only mysql() is accepted there, so the table function points back at this server's own MySQL
-- wire port, the way 02479_mysql_connect_to_self does. Over that wire every column arrives as
-- String, which is what makes the "<old> -> <new>" type changes visible in the report.

SET send_logs_level = 'fatal'; -- a connection try that is retried logs an error of its own
SET external_table_functions_use_nulls = 0; -- keep the wire types bare, so the report stays stable

DROP TABLE IF EXISTS tov_src;
CREATE TABLE tov_src (id UInt32, s String, n Int64, dt DateTime, v Float32) ENGINE = TinyLog;
INSERT INTO tov_src VALUES (1, 'a', 1, '2020-01-01 00:00:00', 1.5);

-- Every line of the report at once: two modified columns (so the report also has to separate two
-- names in one line), one added ALIAS column, and all five key clauses.
EXPLAIN TABLE OVERRIDE mysql(
    concat('127.0.0.1:', toString(getServerPort('mysql_port'))),
    currentDatabase(), 'tov_src', 'default', '',
    SETTINGS connect_timeout = 100, connection_wait_timeout = 100)
COLUMNS (id UInt64, s LowCardinality(String), al UInt32 ALIAS id + 1)
PARTITION BY toYYYYMM(dt)
PRIMARY KEY id
ORDER BY (id, s)
SAMPLE BY id
TTL dt + INTERVAL 1 DAY;

-- A column the override adds without ALIAS is rejected, so no report is printed at all.
EXPLAIN TABLE OVERRIDE mysql(
    concat('127.0.0.1:', toString(getServerPort('mysql_port'))),
    currentDatabase(), 'tov_src', 'default', '',
    SETTINGS connect_timeout = 100, connection_wait_timeout = 100)
COLUMNS (extra UInt8)
ORDER BY id; -- { serverError INVALID_TABLE_OVERRIDE }

DROP TABLE tov_src;
