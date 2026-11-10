-- `bin` and `bin_at` over a table column of the narrower ClickHouse date types: `DateTime`,
-- `Date`, `Date32`. They used to be rejected as "got DateTime and IntervalNanosecond".

DROP TABLE IF EXISTS t_kql_bin_narrow;
CREATE TABLE t_kql_bin_narrow
(
    id UInt8,
    dt DateTime('UTC'),
    dt_n Nullable(DateTime('UTC')),
    d Date,
    d32 Date32
) ENGINE = Memory;

INSERT INTO t_kql_bin_narrow VALUES
    (1, '2026-08-01 12:34:56', '2026-08-01 12:34:56', '2026-08-01', '1950-03-04'),
    (2, '2026-08-02 00:00:01', NULL, '2026-08-02', '1950-03-05');

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

t_kql_bin_narrow | project id, b = bin(dt, 1h), bd = bin(dt, 1d) | sort by id asc;
t_kql_bin_narrow | project id, b = bin(dt_n, 1d) | sort by id asc;
t_kql_bin_narrow | summarize c = count() by b = bin(dt, 1d) | sort by b asc;
t_kql_bin_narrow | where id > 0 | summarize c = count() by bin(dt, 1d) | count;
t_kql_bin_narrow | project id, b = floor(dt, 1d) | sort by id asc;
t_kql_bin_narrow | project id, b = bin_at(dt, 1d, datetime(2026-08-01 06:00:00)) | sort by id asc;
-- the bin is before the Unix epoch, which `DateTime` cannot hold
t_kql_bin_narrow | project id, b = bin_at(dt, 73000d, datetime(1900-01-01)) | sort by id asc;

SET dialect = 'clickhouse';

SELECT id, kqlBin(d, toIntervalNanosecond(7 * 86400000000000)), toTypeName(kqlBin(d, toIntervalDay(1))) FROM t_kql_bin_narrow ORDER BY id SETTINGS session_timezone = 'UTC';
SELECT id, kqlBin(d32, toIntervalDay(1)) FROM t_kql_bin_narrow ORDER BY id SETTINGS session_timezone = 'UTC';
SELECT id, kqlBinAt(dt, toIntervalHour(6), d) FROM t_kql_bin_narrow ORDER BY id SETTINGS session_timezone = 'UTC';
SELECT toTypeName(kqlBin(dt, toIntervalHour(1))) FROM t_kql_bin_narrow LIMIT 1;
-- a calendar-length bin keeps going through `toStartOfInterval`
SELECT id, kqlBin(dt, toIntervalMonth(1)) FROM t_kql_bin_narrow ORDER BY id;

DROP TABLE t_kql_bin_narrow;
