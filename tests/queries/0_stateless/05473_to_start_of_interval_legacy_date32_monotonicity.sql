-- Tags: no-parallel-replicas
-- https://github.com/ClickHouse/ClickHouse/issues/116945
-- With the legacy result types, `toStartOfInterval` and `dateTrunc` narrow a `Date32` argument into `Date` / `DateTime`,
-- which wraps outside their range. Index analysis must not treat such a rounding as monotonic. Each `count()` is next to its truth.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_asc;
DROP TABLE IF EXISTS t_desc;
CREATE TABLE t_asc (d Date32) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, auto_statistics_types = '';
CREATE TABLE t_desc (d Date32) ENGINE = MergeTree ORDER BY d DESC
    SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, auto_statistics_types = '';
INSERT INTO t_asc VALUES ('1900-01-01'), ('1969-12-31'), ('2000-01-01'), ('2149-06-06'), ('2200-01-01'), ('2299-12-31');
INSERT INTO t_desc SELECT * FROM t_asc;

SELECT 'compatibility';
SELECT toTypeName(toStartOfInterval(toDate32('2000-01-01'), INTERVAL 1 YEAR)) SETTINGS compatibility = '26.10';
SELECT count() FROM t_asc WHERE toStartOfInterval(d, INTERVAL 1 YEAR) >= toDate('2100-01-01') SETTINGS compatibility = '26.10';
SELECT countIf(toStartOfInterval(d, INTERVAL 1 YEAR) >= toDate('2100-01-01')) FROM t_asc SETTINGS compatibility = '26.10';

SET to_start_of_interval_preserves_argument_type = 0;
SELECT toTypeName(toStartOfInterval(toDate32('2000-01-01'), INTERVAL 1 YEAR)), toTypeName(toStartOfInterval(toDate32('2000-01-01'), INTERVAL 1 DAY));

SELECT 'boundary values';
SELECT count() FROM t_asc WHERE toStartOfInterval(d, INTERVAL 1 YEAR) >= toDate('2100-01-01');
SELECT count() FROM t_desc WHERE toStartOfInterval(d, INTERVAL 1 YEAR) >= toDate('2100-01-01');
SELECT countIf(toStartOfInterval(d, INTERVAL 1 YEAR) >= toDate('2100-01-01')) FROM t_asc;
SELECT count() FROM t_asc WHERE toStartOfInterval(d, INTERVAL 1 MONTH) >= toDate('2100-01-01');
SELECT count() FROM t_desc WHERE toStartOfInterval(d, INTERVAL 1 MONTH) >= toDate('2100-01-01');
SELECT countIf(toStartOfInterval(d, INTERVAL 1 MONTH) >= toDate('2100-01-01')) FROM t_asc;
SELECT count() FROM t_asc WHERE toStartOfInterval(d, INTERVAL 1 DAY) >= toDateTime('2050-01-01', 'UTC');
SELECT count() FROM t_desc WHERE toStartOfInterval(d, INTERVAL 1 DAY) >= toDateTime('2050-01-01', 'UTC');
SELECT countIf(toStartOfInterval(d, INTERVAL 1 DAY) >= toDateTime('2050-01-01', 'UTC')) FROM t_asc;

SELECT 'exact count with a skip index';
DROP TABLE IF EXISTS t_fuzz;
CREATE TABLE t_fuzz (d Date32) ENGINE = MergeTree ORDER BY d DESC
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, auto_statistics_types = '', add_minmax_index_for_temporal_columns = 1;
INSERT INTO t_fuzz VALUES ('1900-01-01'), ('1969-12-31'), ('1970-01-02'), ('2000-01-01'), ('2106-06-15'), ('2200-01-01'), ('2262-04-11');
SELECT count() FROM t_fuzz WHERE toStartOfInterval(d, INTERVAL 1 YEAR) >= toDate('2000-01-01')
    SETTINGS force_primary_key = 1, use_skip_indexes_on_data_read = 0, optimize_use_implicit_projections = 1;
SELECT countIf(toStartOfInterval(d, INTERVAL 1 YEAR) >= toDate('2000-01-01')) FROM t_fuzz;

SELECT 'intervals longer than one unit';
DROP TABLE IF EXISTS t_epoch;
CREATE TABLE t_epoch (d Date32) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, auto_statistics_types = '';
INSERT INTO t_epoch SELECT toDate32('1970-01-01') + number * 30 FROM numbers(30);
SELECT count() FROM t_epoch WHERE toStartOfInterval(d, INTERVAL 3 YEAR) >= toDate('2100-01-01');
SELECT countIf(toStartOfInterval(d, INTERVAL 3 YEAR) >= toDate('2100-01-01')) FROM t_epoch;
TRUNCATE TABLE t_epoch;
INSERT INTO t_epoch SELECT toDate32('1970-01-01') + number FROM numbers(10);
SELECT count() FROM t_epoch WHERE toStartOfInterval(d, INTERVAL 1 WEEK) = toDate('2149-06-04');
SELECT countIf(toStartOfInterval(d, INTERVAL 1 WEEK) = toDate('2149-06-04')) FROM t_epoch;
SELECT count() FROM t_epoch WHERE toStartOfInterval(d, INTERVAL 1 WEEK) >= toDate('2100-01-01');
SELECT countIf(toStartOfInterval(d, INTERVAL 1 WEEK) >= toDate('2100-01-01')) FROM t_epoch;

SELECT 'dateTrunc';
SELECT count() FROM t_asc WHERE dateTrunc('year', d) >= toDate('2100-01-01') SETTINGS function_date_trunc_return_type_behavior = 1;
SELECT countIf(dateTrunc('year', d) >= toDate('2100-01-01')) FROM t_asc SETTINGS function_date_trunc_return_type_behavior = 1;
SELECT count() FROM t_asc WHERE dateTrunc('day', d) >= toDateTime('2050-01-01', 'UTC') SETTINGS function_date_trunc_return_type_behavior = 1;
SELECT countIf(dateTrunc('day', d) >= toDateTime('2050-01-01', 'UTC')) FROM t_asc SETTINGS function_date_trunc_return_type_behavior = 1;

SELECT 'time zone';
SET session_timezone = 'America/Hermosillo';
DROP TABLE IF EXISTS t_tz;
CREATE TABLE t_tz (d Date32) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, auto_statistics_types = '';
INSERT INTO t_tz VALUES ('2106-02-05'), ('2106-02-06'), ('2106-02-07');
SELECT count() FROM t_tz WHERE toStartOfInterval(d, INTERVAL 1 DAY) >= toDateTime('2106-02-06 00:00:00');
SELECT countIf(toStartOfInterval(d, INTERVAL 1 DAY) >= toDateTime('2106-02-06 00:00:00')) FROM t_tz;
SET session_timezone = 'UTC';

SELECT 'Nullable key';
DROP TABLE IF EXISTS t_nullable;
CREATE TABLE t_nullable (d Nullable(Date32)) ENGINE = MergeTree ORDER BY d
    SETTINGS allow_nullable_key = 1, index_granularity = 1, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, auto_statistics_types = '';
INSERT INTO t_nullable SELECT * FROM t_asc;
INSERT INTO t_nullable VALUES (NULL);
SELECT count() FROM t_nullable WHERE toStartOfInterval(d, INTERVAL 1 YEAR) >= toDate('2100-01-01');
SELECT countIf(toStartOfInterval(d, INTERVAL 1 YEAR) >= toDate('2100-01-01')) FROM t_nullable;
SELECT count() FROM t_nullable WHERE toStartOfInterval(d, INTERVAL 1 DAY) >= toDateTime('2050-01-01', 'UTC');
SELECT countIf(toStartOfInterval(d, INTERVAL 1 DAY) >= toDateTime('2050-01-01', 'UTC')) FROM t_nullable;

SELECT 'order';
SELECT arraySort(groupArray(x)) = groupArray(x)
FROM (SELECT toStartOfInterval(d, INTERVAL 1 YEAR) AS x FROM t_asc ORDER BY x SETTINGS optimize_read_in_order = 1);

SELECT 'order, LowCardinality(Nullable) key';
SET allow_suspicious_low_cardinality_types = 1;
DROP TABLE IF EXISTS t_lc_nullable;
CREATE TABLE t_lc_nullable (d LowCardinality(Nullable(Date32))) ENGINE = MergeTree ORDER BY d
    SETTINGS allow_nullable_key = 1, index_granularity = 1, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, auto_statistics_types = '';
INSERT INTO t_lc_nullable VALUES ('1900-01-01'), ('1969-12-31'), ('2000-01-01'), ('2149-06-06'), ('2200-01-01'), ('2299-12-31'), (NULL);
SELECT arraySort(groupArray(x)) = groupArray(x)
FROM (SELECT toStartOfInterval(d, INTERVAL 1 YEAR) AS x FROM t_lc_nullable ORDER BY x SETTINGS optimize_read_in_order = 1);
SELECT arraySort(groupArray(x)) = groupArray(x)
FROM (SELECT dateTrunc('year', d) AS x FROM t_lc_nullable ORDER BY x SETTINGS optimize_read_in_order = 1, function_date_trunc_return_type_behavior = 1);

SELECT 'the primary key is still used where the rounding fits';
DROP TABLE IF EXISTS t_fits;
CREATE TABLE t_fits (d Date32) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, auto_statistics_types = '';
INSERT INTO t_fits SELECT toDate32('2000-01-01') + number FROM numbers(100);
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_fits WHERE toStartOfInterval(d, INTERVAL 1 DAY) >= toDateTime('2000-03-01', 'UTC'))
WHERE explain ILIKE '%Granules: 41/100%';
TRUNCATE TABLE t_fits;
INSERT INTO t_fits SELECT toDate32('2149-06-07') + number FROM numbers(24);
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_fits WHERE toStartOfInterval(d, INTERVAL 1 YEAR) < toDate('2149-01-01'))
WHERE explain ILIKE '%Granules: 0/24%';

DROP TABLE t_asc;
DROP TABLE t_desc;
DROP TABLE t_fuzz;
DROP TABLE t_epoch;
DROP TABLE t_tz;
DROP TABLE t_nullable;
DROP TABLE t_lc_nullable;
DROP TABLE t_fits;
