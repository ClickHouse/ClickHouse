-- Tags: no-fasttest
-- no-fasttest: 'countmin' sketches need a 3rd party library

-- `uniq` and `uniq_v2` statistics are supported on UUID columns (and on Nullable / LowCardinality of UUID).
-- They provide the number of distinct values (NDV) which is used by the planner, e.g. for join reordering.
-- `tdigest`, `minmax` and `countmin` are still not supported for UUID.

SET allow_statistics = 1;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET mutations_sync = 2;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_uuid_stats SYNC;
DROP TABLE IF EXISTS t_uuid_stats_alter SYNC;

SELECT 'Statistics declared in CREATE TABLE';

CREATE TABLE t_uuid_stats
(
    id UInt64,
    u UUID STATISTICS(uniq),
    u2 UUID STATISTICS(uniq_v2),
    nu Nullable(UUID) STATISTICS(uniq),
    lu LowCardinality(UUID) STATISTICS(uniq_v2),
    lnu LowCardinality(Nullable(UUID)) STATISTICS(uniq, uniq_v2),
    plain UUID
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 0, auto_statistics_types = '';

-- 100 distinct UUIDs, 100 rows each.
INSERT INTO t_uuid_stats
SELECT
    number,
    toUUID(concat('00000000-0000-0000-0000-', lpad(hex(number % 100), 12, '0'))) AS v,
    v,
    if(number % 7 = 0, NULL, v),
    v,
    if(number % 7 = 0, NULL, v),
    v
FROM numbers(10000);

SELECT column, type, notEmpty(statistics) AS has_statistics, ifNull(estimates.cardinality, 0) BETWEEN 90 AND 110 AS ndv_is_about_100
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_uuid_stats' AND active AND column IN ('u', 'u2', 'nu', 'lu', 'lnu')
ORDER BY column;

SELECT 'Column without statistics';
SELECT column, empty(statistics) AS no_statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_uuid_stats' AND active AND column = 'plain';

SELECT 'ALTER ADD / MATERIALIZE / DROP statistics';

CREATE TABLE t_uuid_stats_alter (id UInt64, u UUID)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 0, auto_statistics_types = '';

INSERT INTO t_uuid_stats_alter SELECT number, toUUID(concat('00000000-0000-0000-0000-', lpad(hex(number % 100), 12, '0'))) FROM numbers(10000);

ALTER TABLE t_uuid_stats_alter ADD STATISTICS u TYPE uniq, uniq_v2;
ALTER TABLE t_uuid_stats_alter MATERIALIZE STATISTICS u;

SELECT column, notEmpty(statistics) AS has_statistics, ifNull(estimates.cardinality, 0) BETWEEN 90 AND 110 AS ndv_is_about_100
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_uuid_stats_alter' AND active AND column = 'u';

ALTER TABLE t_uuid_stats_alter DROP STATISTICS u;

SELECT column, empty(statistics) AS no_statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_uuid_stats_alter' AND active AND column = 'u';

SELECT 'Other statistics types are still rejected for UUID';
ALTER TABLE t_uuid_stats_alter ADD STATISTICS u TYPE tdigest; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE t_uuid_stats_alter ADD STATISTICS u TYPE countmin; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE t_uuid_stats_alter ADD STATISTICS u TYPE minmax; -- { serverError ILLEGAL_STATISTICS }

SELECT 'Queries that use the statistics return correct results';
ALTER TABLE t_uuid_stats_alter ADD STATISTICS u TYPE uniq;
ALTER TABLE t_uuid_stats_alter MATERIALIZE STATISTICS u;

SELECT count() FROM t_uuid_stats WHERE u = toUUID('00000000-0000-0000-0000-000000000005');
SELECT count() FROM t_uuid_stats WHERE u = '00000000-0000-0000-0000-000000000005' AND nu IS NOT NULL;
SELECT count() FROM t_uuid_stats WHERE u IN (toUUID('00000000-0000-0000-0000-000000000001'), toUUID('00000000-0000-0000-0000-000000000002'));
SELECT count() FROM t_uuid_stats WHERE u < toUUID('00000000-0000-0000-0000-000000000002');

-- Join on a UUID key: the NDV is available for both sides.
SET query_plan_optimize_join_order_limit = 10;
SELECT count() FROM t_uuid_stats_alter AS a INNER JOIN t_uuid_stats AS b ON a.u = b.u WHERE a.id < 100;
SELECT count() FROM t_uuid_stats AS b INNER JOIN t_uuid_stats_alter AS a ON a.u = b.u WHERE a.id < 100;

DROP TABLE t_uuid_stats SYNC;
DROP TABLE t_uuid_stats_alter SYNC;
