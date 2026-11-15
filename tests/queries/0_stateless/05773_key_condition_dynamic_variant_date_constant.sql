-- A comparison with a `Dynamic` or `Variant` constant runs on the constant's active member, so the primary key,
-- a minmax skip index and statistics part pruning must analyze that member, not the wrapper type.
-- `DateTime('Europe/Moscow') = Date32` compares both sides as `DateTime64(0)` in the session time zone.

SET session_timezone = 'UTC';
SET use_query_condition_cache = 0;
SET explain_query_plan_default = 'legacy';
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS t_pk;
DROP TABLE IF EXISTS t_src;
DROP TABLE IF EXISTS t_nullable;
DROP TABLE IF EXISTS t_d32;
DROP TABLE IF EXISTS t_mm;
DROP TABLE IF EXISTS t_stats;
DROP TABLE IF EXISTS t_tuple;
DROP TABLE IF EXISTS t_str;
DROP TABLE IF EXISTS t_u64;
DROP TABLE IF EXISTS t_enum;
DROP TABLE IF EXISTS t_nullable_u64;

CREATE TABLE t_pk (dt DateTime('Europe/Moscow')) ENGINE = MergeTree ORDER BY dt
SETTINGS index_granularity = 1, auto_statistics_types = '';
INSERT INTO t_pk SELECT toDateTime('2024-01-02 00:00:00', 'UTC') + number * 86400 FROM numbers(3);

SELECT '-- primary key';
SELECT (SELECT count() FROM t_pk WHERE dt = CAST(toDate32('2024-01-02') AS Dynamic)) AS pruned,
       (SELECT countIf(dt = CAST(toDate32('2024-01-02') AS Dynamic)) FROM t_pk) AS honest;
SELECT count() FROM t_pk WHERE dt = CAST(toDate32('2024-01-02') AS Variant(Date32, String));
SELECT count() FROM t_pk WHERE dt = CAST(toDate32('2024-01-02') AS Dynamic(max_types = 0));
SELECT count() FROM t_pk WHERE dt <= CAST(toDate32('2024-01-03') AS Dynamic);
SELECT count() FROM t_pk WHERE dt != CAST(toDate32('2024-01-02') AS Dynamic);

CREATE TABLE t_src (d Dynamic) ENGINE = Memory;
INSERT INTO t_src SELECT toDate32('2024-01-02');
SELECT count() FROM t_pk WHERE dt = (SELECT d FROM t_src);

SELECT trim(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_pk WHERE dt = CAST(toDate32('2024-01-02') AS Dynamic))
WHERE trim(explain) ILIKE 'granules:%';
SELECT trim(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_pk WHERE dt <= CAST(toDate32('2024-01-03') AS Dynamic))
WHERE trim(explain) ILIKE 'granules:%';

CREATE TABLE t_nullable (dt Nullable(DateTime('Europe/Moscow'))) ENGINE = MergeTree ORDER BY dt
SETTINGS index_granularity = 1, allow_nullable_key = 1, auto_statistics_types = '';
INSERT INTO t_nullable SELECT toDateTime('2024-01-02 00:00:00', 'UTC') + number * 86400 FROM numbers(3);
SELECT count() FROM t_nullable WHERE dt = CAST(toDate32('2024-01-02') AS Dynamic);

CREATE TABLE t_d32 (d Date32) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1, auto_statistics_types = '';
INSERT INTO t_d32 SELECT toDate32('2024-01-01') + number FROM numbers(3);
SELECT count() FROM t_d32 WHERE d = CAST(toDateTime('2024-01-01 19:00:00', 'America/New_York') AS Dynamic);
SELECT count() FROM t_d32 WHERE d = CAST(toDateTime('2024-01-01 19:00:00', 'America/New_York') AS Variant(DateTime('America/New_York'), String));

SELECT '-- minmax skip index';
CREATE TABLE t_mm (id UInt64, dt DateTime('Europe/Moscow'), INDEX mm dt TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, auto_statistics_types = '';
INSERT INTO t_mm SELECT number, toDateTime('2024-01-02 00:00:00', 'UTC') + number * 86400 FROM numbers(3);
SELECT count() FROM t_mm WHERE dt = CAST(toDate32('2024-01-02') AS Dynamic);
SELECT trim(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_mm WHERE dt = CAST(toDate32('2024-01-02') AS Dynamic) SETTINGS use_skip_indexes_on_data_read = 0)
WHERE trim(explain) ILIKE 'granules:%';

SELECT '-- statistics part pruning';
CREATE TABLE t_stats (dt DateTime('Europe/Moscow')) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic';
SET materialize_statistics_on_insert = 1;
INSERT INTO t_stats SELECT toDateTime('2024-01-02 00:00:00', 'UTC');
INSERT INTO t_stats SELECT toDateTime('2024-01-03 00:00:00', 'UTC');
INSERT INTO t_stats SELECT toDateTime('2024-01-04 00:00:00', 'UTC');
SELECT count() FROM t_stats WHERE dt = CAST(toDate32('2024-01-02') AS Dynamic)
SETTINGS use_statistics_for_part_pruning = 1, min_marks_per_part_to_load_statistics = 0;
SELECT count() FROM t_stats WHERE dt = CAST(toDate32('2024-01-02') AS Variant(Date32, String))
SETTINGS use_statistics_for_part_pruning = 1, min_marks_per_part_to_load_statistics = 0;

SELECT '-- composite member';
-- The member keeps the types nested in it: here a `Tuple(Dynamic)` holding a `Date32`.
CREATE TABLE t_tuple (k Tuple(DateTime('UTC'))) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 1, auto_statistics_types = '';
INSERT INTO t_tuple SELECT tuple(toDateTime('2024-01-02 00:00:00', 'UTC') + number * 86400) FROM numbers(3);
SELECT (SELECT count() FROM t_tuple WHERE k = CAST(tuple(CAST(toDate32('2024-01-02') AS Dynamic)) AS Dynamic)) AS pruned,
       (SELECT countIf(k = CAST(tuple(CAST(toDate32('2024-01-02') AS Dynamic)) AS Dynamic)) FROM t_tuple) AS honest;
SELECT (SELECT count() FROM t_tuple WHERE k = CAST(tuple(CAST(toDate32('2024-01-02') AS Dynamic)) AS Dynamic(max_types = 0))) AS pruned,
       (SELECT countIf(k = CAST(tuple(CAST(toDate32('2024-01-02') AS Dynamic)) AS Dynamic(max_types = 0))) FROM t_tuple) AS honest;
SELECT (SELECT count() FROM t_tuple WHERE k = CAST(tuple(CAST(toDate32('2024-01-02') AS Dynamic)) AS Variant(Tuple(Dynamic), String))) AS pruned,
       (SELECT countIf(k = CAST(tuple(CAST(toDate32('2024-01-02') AS Dynamic)) AS Variant(Tuple(Dynamic), String))) FROM t_tuple) AS honest;

SELECT '-- custom-named wrapper';
SELECT count() FROM t_pk
WHERE dt = CAST(CAST(toDate32('2024-01-02') AS Variant(Date32, String)) AS SimpleAggregateFunction(anyLast, Variant(Date32, String)));

SELECT '-- pattern operand';
-- `like` does not accept an `Enum8` pattern, so the query throws instead of pruning every granule.
CREATE TABLE t_str (s String) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1, auto_statistics_types = '';
INSERT INTO t_str VALUES ('b'), ('c');
SELECT count() FROM t_str WHERE s LIKE CAST(CAST('a' AS Enum8('a' = 1)) AS Dynamic); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- incomparable member';
CREATE TABLE t_u64 (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, auto_statistics_types = '';
INSERT INTO t_u64 VALUES (1), (2), (3);
SELECT count() FROM t_u64 WHERE k != CAST(toDate32('2024-01-02') AS Variant(Date32, String)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM t_u64 WHERE k = CAST(toDate32('2024-01-02') AS Dynamic); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- string member';
-- A string member is compared with an enum key by label, not converted to the enum as a constant string is.
CREATE TABLE t_enum (k Enum8('a' = 2, 'b' = 1, 'c' = 3)) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 1, auto_statistics_types = '';
INSERT INTO t_enum VALUES ('a'), ('b'), ('c');
SELECT (SELECT count() FROM t_enum WHERE k < CAST('b' AS Dynamic)) AS pruned,
       (SELECT countIf(k < CAST('b' AS Dynamic)) FROM t_enum) AS honest;
SELECT count() FROM t_enum WHERE k < CAST('b' AS Dynamic(max_types = 0));
SELECT count() FROM t_enum WHERE k < CAST('b' AS Variant(String, UInt8));

SELECT '-- numeric member';
-- `UInt64` and `Int64` have no common type but are compared.
CREATE TABLE t_nullable_u64 (k Nullable(UInt64)) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 1, allow_nullable_key = 1, auto_statistics_types = '';
INSERT INTO t_nullable_u64 VALUES (1), (2), (3);
SELECT count() FROM t_nullable_u64 WHERE k > CAST(toInt64(1) AS Dynamic);
SELECT count() FROM t_nullable_u64 WHERE k > CAST(toInt64(1) AS Dynamic(max_types = 0));

DROP TABLE t_pk;
DROP TABLE t_src;
DROP TABLE t_nullable;
DROP TABLE t_d32;
DROP TABLE t_mm;
DROP TABLE t_stats;
DROP TABLE t_tuple;
DROP TABLE t_str;
DROP TABLE t_u64;
DROP TABLE t_enum;
DROP TABLE t_nullable_u64;
