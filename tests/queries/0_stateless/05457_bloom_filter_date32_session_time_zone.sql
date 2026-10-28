-- Searching a `DateTime` column with a time zone for a `Date32` constant compares both in their least supertype, a
-- `DateTime64` in the session time zone, so the `bloom_filter` index must probe that instant, not the date's midnight
-- in the column's time zone. The `m['k'] = <date>` path hashed the constant without converting it at all.
SET session_timezone = 'UTC';
SET use_skip_indexes = 1;

DROP TABLE IF EXISTS t_bf_date32_tz;

CREATE TABLE t_bf_date32_tz
(
    id UInt64,
    dt DateTime('Europe/Moscow'),
    arr Array(DateTime('Europe/Moscow')),
    mk Map(DateTime('Europe/Moscow'), UInt8),
    mv Map(String, DateTime('Europe/Moscow')),
    INDEX i_dt dt TYPE bloom_filter GRANULARITY 1,
    INDEX i_arr arr TYPE bloom_filter GRANULARITY 1,
    INDEX i_mk mapKeys(mk) TYPE bloom_filter GRANULARITY 1,
    INDEX i_mv mapValues(mv) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

INSERT INTO t_bf_date32_tz SELECT number, toDateTime('2030-05-05 05:05:05', 'UTC') + number AS x, [x], map(x, 1), map('k', x) FROM numbers(40) WHERE number NOT IN (21, 30);
INSERT INTO t_bf_date32_tz SELECT 21, x, [x], map(x, 1), map('k', x) FROM (SELECT toDateTime('2024-01-02 00:00:00', 'UTC') AS x);
INSERT INTO t_bf_date32_tz SELECT 30, x, [x], map(x, 1), map('k', x) FROM (SELECT toDateTime('2024-01-05 00:00:00', 'Europe/Moscow') AS x);
INSERT INTO t_bf_date32_tz SELECT 40, x, [x], map(x, 1), map('other', x) FROM (SELECT toDateTime('2030-06-06 06:06:06', 'UTC') AS x);
OPTIMIZE TABLE t_bf_date32_tz FINAL;

SELECT count() FROM t_bf_date32_tz WHERE hasAny(arr, [toDate32('2024-01-02')]) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_date32_tz WHERE hasAny(arr, [toDate32('2024-01-02')]);
SELECT count() FROM t_bf_date32_tz WHERE hasAll(arr, [toDate32('2024-01-02')]);
SELECT count() FROM t_bf_date32_tz WHERE has(arr, toDate32('2024-01-02'));
SELECT count() FROM t_bf_date32_tz WHERE indexOf(arr, toDate32('2024-01-02')) = 1;
SELECT count() FROM t_bf_date32_tz WHERE dt = toDate32('2024-01-02');
SELECT count() FROM t_bf_date32_tz WHERE has([toDate32('2024-01-02')], dt);
SELECT count() FROM t_bf_date32_tz WHERE mapContainsKey(mk, toDate32('2024-01-02'));
SELECT count() FROM t_bf_date32_tz WHERE mapContainsValue(mv, toDate32('2024-01-02'));
SELECT count() FROM t_bf_date32_tz WHERE mv['k'] = toDate32('2024-01-02');
SELECT count() FROM t_bf_date32_tz WHERE mv['k'] = toDate('2024-01-05');
-- A missing key yields `1970-01-01 00:00:00`, which equals this constant.
SELECT count() FROM t_bf_date32_tz WHERE mv['k'] = toDate32('1970-01-01');
SELECT count() FROM t_bf_date32_tz WHERE mv['k'] = toDateTime64('2024-01-02 00:00:00', 0, 'UTC');

DROP TABLE t_bf_date32_tz;
