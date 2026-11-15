-- The Date 1970-01-01 starts before the Unix epoch in a time zone east of UTC.
SET session_timezone = 'Europe/Berlin';

SELECT toRelativeMinuteNum(toDate('1970-01-01')), toRelativeMinuteNum(toDate('1970-01-02')), toRelativeMinuteNum(toDate32('1970-01-01'));
SELECT toRelativeHourNum(toDate('1970-01-01'), 'Pacific/Tongatapu'), toRelativeHourNum(toDate('1970-01-02'), 'Pacific/Tongatapu');

SELECT dateDiff('minute', d0, d1), dateDiff('second', d0, d1), dateDiff('millisecond', d0, d1), dateDiff('microsecond', d0, d1), dateDiff('nanosecond', d0, d1), dateDiff('hour', d0, d1, 'Pacific/Tongatapu')
FROM (SELECT toDate('1970-01-01') AS d0, toDate('1970-01-02') AS d1);
SELECT age('minute', d0, d1), age('second', d0, d1), age('millisecond', d0, d1), age('hour', d0, d1, 'Pacific/Tongatapu'), timeDiff(d0, d1)
FROM (SELECT toDate('1970-01-01') AS d0, toDate('1970-01-02') AS d1);
SELECT dateDiff('second', d0, d1), age('second', d0, d1), timeDiff(d0, d1)
FROM (SELECT toDate('2120-01-01') AS d0, toDate('2120-01-02') AS d1);

-- Each max_rows_to_read bound fails unless the index or the statistics prune the other granules, parts or partitions.
DROP TABLE IF EXISTS t_pk;
CREATE TABLE t_pk (d Date) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 64, auto_statistics_types = '';
INSERT INTO t_pk SELECT toDate(number) FROM numbers(349);
SELECT d FROM t_pk WHERE toRelativeMinuteNum(d) = toRelativeMinuteNum(toDate('1970-01-01')) SETTINGS max_rows_to_read = 64;
SELECT count() FROM t_pk WHERE toRelativeMinuteNum(d) < 1440;
SELECT count() FROM t_pk WHERE toRelativeMinuteNum(d) = toRelativeMinuteNum(toDate('1970-01-02')) SETTINGS force_primary_key = 1;
SELECT toRelativeMinuteNum(d) FROM t_pk ORDER BY toRelativeMinuteNum(d) LIMIT 3 SETTINGS optimize_read_in_order = 1;
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1, pretty = 0 SELECT toRelativeMinuteNum(d) FROM t_pk ORDER BY toRelativeMinuteNum(d) LIMIT 3 SETTINGS optimize_read_in_order = 1) WHERE explain LIKE '%ReadType: InOrder%';
DROP TABLE t_pk;

DROP TABLE IF EXISTS t_stat;
CREATE TABLE t_stat (d Date, k UInt32) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'minmax';
SYSTEM STOP MERGES t_stat;
SET materialize_statistics_on_insert = 1;
INSERT INTO t_stat SELECT toDate(number), number FROM numbers(349);
INSERT INTO t_stat SELECT toDate(1000 + number), 1000 + number FROM numbers(100);
SELECT d FROM t_stat WHERE toRelativeMinuteNum(d) = toRelativeMinuteNum(toDate('1970-01-01')) SETTINGS use_statistics_for_part_pruning = 1, min_marks_per_part_to_load_statistics = 0, max_rows_to_read = 349;
DROP TABLE t_stat;

DROP TABLE IF EXISTS t_part;
CREATE TABLE t_part (d Date, k UInt32) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_part SELECT toDate(number), number FROM numbers(349);
SELECT d FROM t_part WHERE toRelativeMinuteNum(d) = toRelativeMinuteNum(toDate('1970-01-01')) SETTINGS force_index_by_date = 1, max_rows_to_read = 31;
DROP TABLE t_part;
