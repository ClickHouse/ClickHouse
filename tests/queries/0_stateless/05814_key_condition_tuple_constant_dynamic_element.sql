-- A `Tuple` compares a `Dynamic` or `Variant` element as its active member, so key analysis must not use the constant cast to the key type.

SET session_timezone = 'UTC';
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_pk;
DROP TABLE IF EXISTS t_mm;
DROP TABLE IF EXISTS t_d32;

CREATE TABLE t_pk (k Tuple(DateTime('Europe/Moscow'))) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, auto_statistics_types = '';
INSERT INTO t_pk SELECT tuple(toDateTime('2024-01-02 00:00:00', 'UTC') + number * 86400) FROM numbers(3);
SELECT 'pk Dynamic', count() FROM t_pk WHERE k = tuple(CAST(toDate32('2024-01-02'), 'Dynamic'));
SELECT 'pk Variant', count() FROM t_pk WHERE k = tuple(CAST(toDate32('2024-01-02'), 'Variant(Date32, String)'));
SELECT 'pk plain', count() FROM t_pk WHERE k = tuple(toDateTime('2024-01-02 00:00:00', 'UTC')) SETTINGS force_primary_key = 1;

CREATE TABLE t_mm (id UInt64, k Tuple(DateTime('Europe/Moscow')), INDEX mm k TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, auto_statistics_types = '';
INSERT INTO t_mm SELECT number, tuple(toDateTime('2024-01-02 00:00:00', 'UTC') + number * 86400) FROM numbers(3);
SELECT 'mm Dynamic', count() FROM t_mm WHERE k = tuple(CAST(toDate32('2024-01-02'), 'Dynamic'));

CREATE TABLE t_d32 (d Tuple(Date32)) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1, auto_statistics_types = '';
INSERT INTO t_d32 SELECT tuple(toDate32('2024-01-01') + number) FROM numbers(3);
SELECT 'd32 Dynamic', count() FROM t_d32 WHERE d = tuple(CAST(toDateTime('2024-01-01 19:00:00', 'America/New_York'), 'Dynamic'));

DROP TABLE t_pk;
DROP TABLE t_mm;
DROP TABLE t_d32;
