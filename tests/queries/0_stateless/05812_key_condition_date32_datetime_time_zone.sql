-- A `Date32` is compared with a `DateTime` as their least supertype `DateTime64(0)`, i.e. in the session time zone.
SET session_timezone = 'UTC';
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_part;
DROP TABLE IF EXISTS t_pk;
DROP TABLE IF EXISTS t_hash;
DROP TABLE IF EXISTS t_hex;
DROP TABLE IF EXISTS t_tuple;
DROP TABLE IF EXISTS t_d32;

CREATE TABLE t_part (dt DateTime('America/New_York')) ENGINE = MergeTree PARTITION BY toDate(dt) ORDER BY tuple() SETTINGS auto_statistics_types = '';
INSERT INTO t_part SELECT toDateTime('2023-12-31 20:00:00', 'America/New_York') + number * 3600 FROM numbers(34);
SELECT 'part >=', count() FROM t_part WHERE dt >= toDate32('2024-01-02');
SELECT 'part =', count() FROM t_part WHERE dt = toDate32('2024-01-02');
SELECT extract(explain, 'Condition: .*') FROM (EXPLAIN indexes = 1 SELECT dt FROM t_part WHERE dt >= toDate32('2024-01-02')
    SETTINGS optimize_use_implicit_projections = 0) WHERE explain ILIKE '%Condition: %toDate(dt)%';
-- With a `Date` or a `DateTime64` constant the supertype is one of the two sides, so the constant is not converted.
SELECT 'part = Date', count() FROM t_part WHERE dt = toDate('2024-01-02');
SELECT extract(explain, 'Condition: .*') FROM (EXPLAIN indexes = 1 SELECT dt FROM t_part WHERE dt = toDate('2024-01-02')
    SETTINGS optimize_use_implicit_projections = 0) WHERE explain ILIKE '%Condition: %toDate(dt)%';
SELECT 'part = DateTime64', count() FROM t_part WHERE dt = toDateTime64('2024-01-02 10:00:00', 0, 'Asia/Tokyo');
SELECT extract(explain, 'Condition: .*') FROM (EXPLAIN indexes = 1 SELECT dt FROM t_part WHERE dt = toDateTime64('2024-01-02 10:00:00', 0, 'Asia/Tokyo')
    SETTINGS optimize_use_implicit_projections = 0) WHERE explain ILIKE '%Condition: %toDate(dt)%';

CREATE TABLE t_pk (id UInt64, dt DateTime('Europe/Moscow'), INDEX mm toStartOfHour(dt) TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY toStartOfHour(dt) SETTINGS index_granularity = 1, auto_statistics_types = '';
INSERT INTO t_pk SELECT number, toDateTime('2024-01-01 22:00:00', 'UTC') + number * 3600 FROM numbers(6);
SELECT 'pk =', count() FROM t_pk WHERE dt = toDate32('2024-01-02');
SELECT 'pk <', count() FROM t_pk WHERE dt < toDate32('2024-01-02');
SELECT 'mm =', count() FROM t_pk WHERE dt = toDate32('2024-01-02') SETTINGS use_primary_key = 0, force_data_skipping_indices = 'mm', use_skip_indexes = 1;
SELECT extract(explain, 'Condition: .*') FROM (EXPLAIN indexes = 1 SELECT dt FROM t_pk WHERE dt = toDate32('2024-01-02')
    SETTINGS optimize_use_implicit_projections = 0, use_skip_indexes = 0) WHERE explain ILIKE '%Condition: %toStartOfHour(dt)%';

CREATE TABLE t_hash (dt DateTime('Europe/Moscow')) ENGINE = MergeTree ORDER BY cityHash64(dt) SETTINGS index_granularity = 1, auto_statistics_types = '';
INSERT INTO t_hash SELECT toDateTime('2024-01-02 00:00:00', 'UTC') + number * 3600 FROM numbers(100);
SELECT 'hash =', count() FROM t_hash WHERE dt = toDate32('2024-01-02') SETTINGS force_primary_key = 1;
SELECT 'hash <=>', count() FROM t_hash WHERE dt <=> toDate32('2024-01-02') SETTINGS force_primary_key = 1;
-- The result depends on the session time zone, so the query condition cache must not reuse it across zones.
-- `max_block_size = 1` puts every granule in a block of its own, so each granule without matches is cached.
SELECT 'qcc UTC', count() FROM t_hash WHERE dt >= toDate32('2024-01-03')
    SETTINGS use_query_condition_cache = 1, session_timezone = 'UTC', max_block_size = 1;
SELECT 'qcc Asia/Tokyo', count() FROM t_hash WHERE dt >= toDate32('2024-01-03')
    SETTINGS use_query_condition_cache = 1, session_timezone = 'Asia/Tokyo', max_block_size = 1;
CREATE TABLE t_hex (dt DateTime('Europe/Moscow')) ENGINE = MergeTree PARTITION BY hex(dt) ORDER BY tuple() SETTINGS auto_statistics_types = '';
INSERT INTO t_hex SELECT toDateTime('2024-01-01 19:00:00', 'UTC') + number * 3600 FROM numbers(4);
SELECT 'hex !=', count() FROM t_hex WHERE dt != toDate32('2024-01-02');
SELECT extract(explain, 'Condition: .*') FROM (EXPLAIN indexes = 1 SELECT dt FROM t_hex WHERE dt != toDate32('2024-01-02')
    SETTINGS optimize_use_implicit_projections = 0) WHERE explain ILIKE '%Condition: %hex(dt)%';
CREATE TABLE t_tuple (t Tuple(DateTime('Europe/Moscow'), Date)) ENGINE = MergeTree ORDER BY cityHash64(t) SETTINGS index_granularity = 1, auto_statistics_types = '';
INSERT INTO t_tuple SELECT (toDateTime('2024-01-02 00:00:00', 'UTC'), toDate('2024-01-01'));
SELECT 'tuple =', count() FROM t_tuple WHERE t = (toDate32('2024-01-02'), '2024-01-01') SETTINGS force_primary_key = 1;

-- The other direction: a `DateTime` constant against a `Date32` key reads the date in the session time zone.
CREATE TABLE t_d32 (d Date32) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY cityHash64(d) SETTINGS index_granularity = 1, auto_statistics_types = '';
INSERT INTO t_d32 SELECT toDate32('2024-01-30') + number FROM numbers(4);
SELECT 'd32 part =', count() FROM t_d32 WHERE d = toDateTime('2024-01-31 19:00:00', 'America/New_York') SETTINGS use_primary_key = 0;
SELECT extract(explain, 'Condition: .*') FROM (EXPLAIN indexes = 1 SELECT d FROM t_d32 WHERE d = toDateTime('2024-01-31 19:00:00', 'America/New_York')
    SETTINGS optimize_use_implicit_projections = 0, use_primary_key = 0) WHERE explain ILIKE '%Condition: %toYYYYMM(d)%';
SELECT 'd32 hash =', count() FROM t_d32 WHERE d = toDateTime('2024-01-31 19:00:00', 'America/New_York') SETTINGS use_partition_pruning = 0, force_primary_key = 1;

DROP TABLE t_part;
DROP TABLE t_pk;
DROP TABLE t_hash;
DROP TABLE t_hex;
DROP TABLE t_tuple;
DROP TABLE t_d32;
