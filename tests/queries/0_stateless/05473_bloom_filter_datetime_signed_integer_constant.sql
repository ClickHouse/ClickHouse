-- A signed integer constant compared with a `DateTime` must not make the `bloom_filter` index throw
-- `TYPE_MISMATCH`, neither over a plain column nor over `mapValues` of a map.

DROP TABLE IF EXISTS t_bf_datetime_signed;

CREATE TABLE t_bf_datetime_signed
(
    d DateTime,
    m Map(String, DateTime),
    INDEX bf_d d TYPE bloom_filter GRANULARITY 1,
    INDEX bf_m mapValues(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO t_bf_datetime_signed SELECT toDateTime(number, 'UTC'), map('k', toDateTime(number, 'UTC')) FROM numbers(1, 3);

SELECT 'plain';
SELECT count() FROM t_bf_datetime_signed WHERE d = toInt64(1) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_datetime_signed WHERE d = toInt64(1);
SELECT count() FROM t_bf_datetime_signed WHERE d = toInt64(-1);
SELECT count() FROM t_bf_datetime_signed WHERE d = toInt64(100) SETTINGS force_data_skipping_indices = 'bf_d';

SELECT 'map';
SELECT count() FROM t_bf_datetime_signed WHERE m['k'] = toInt64(1) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_datetime_signed WHERE m['k'] = toInt64(1);
SELECT count() FROM t_bf_datetime_signed WHERE m['k'] = toInt64(1) SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_bf_datetime_signed WHERE m['k'] = toInt64(-1);
SELECT count() FROM t_bf_datetime_signed WHERE m['k'] = toInt64(100) SETTINGS force_data_skipping_indices = 'bf_m';

DROP TABLE t_bf_datetime_signed;
