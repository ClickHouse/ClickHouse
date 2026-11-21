-- `toDayOfWeek` is monotonic inside a week that starts on its first day. The index analysis knows the weeks that start
-- on Monday and on Sunday, so with `week_functions_starting_day = 'sunday'` the primary key still prunes, and a filter
-- returns the same rows as without the primary key.

CREATE TABLE t_week_key (d Date) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
-- Six weeks, from Monday 2025-12-01.
INSERT INTO t_week_key SELECT toDate('2025-12-01') + number FROM numbers(42);

SET week_functions_starting_day = 'sunday';

SELECT 'no mode, Sunday = 1';
SELECT count(), groupArray(d) FROM (SELECT d FROM t_week_key WHERE toDayOfWeek(d) = 1 ORDER BY d);
SELECT count(), groupArray(d) FROM (SELECT d FROM t_week_key WHERE toDayOfWeek(d) = 1 ORDER BY d) SETTINGS use_primary_key = 0;
SELECT count() FROM t_week_key WHERE toDayOfWeek(d) >= 6;
SELECT count() FROM t_week_key WHERE toDayOfWeek(d) >= 6 SETTINGS use_primary_key = 0;
SELECT count() FROM t_week_key WHERE toDayOfWeek(d) <= 2;
SELECT count() FROM t_week_key WHERE toDayOfWeek(d) <= 2 SETTINGS use_primary_key = 0;

SELECT 'mode 2, Sunday = 0';
SELECT count() FROM t_week_key WHERE toDayOfWeek(d, 2) = 1;
SELECT count() FROM t_week_key WHERE toDayOfWeek(d, 2) = 1 SETTINGS use_primary_key = 0;
SELECT count() FROM t_week_key WHERE toDayOfWeek(d, 2) >= 6;
SELECT count() FROM t_week_key WHERE toDayOfWeek(d, 2) >= 6 SETTINGS use_primary_key = 0;
SELECT count() FROM t_week_key WHERE toDayOfWeek(d, 2) <= 2;
SELECT count() FROM t_week_key WHERE toDayOfWeek(d, 2) <= 2 SETTINGS use_primary_key = 0;

SELECT 'mode 0, Monday = 1';
SELECT count() FROM t_week_key WHERE toDayOfWeek(d, 0) = 1;
SELECT count() FROM t_week_key WHERE toDayOfWeek(d, 0) = 1 SETTINGS use_primary_key = 0;
SELECT count() FROM t_week_key WHERE toDayOfWeek(d, 0) >= 6;
SELECT count() FROM t_week_key WHERE toDayOfWeek(d, 0) >= 6 SETTINGS use_primary_key = 0;
SELECT count() FROM t_week_key WHERE toDayOfWeek(d, 0) <= 2;
SELECT count() FROM t_week_key WHERE toDayOfWeek(d, 0) <= 2 SETTINGS use_primary_key = 0;

-- How many of the 14 granules (3 days each) the primary key keeps, from `EXPLAIN indexes = 1`. A granule is pruned when
-- its first day and the first day of the next granule lie in the same Monday-to-Saturday run and the filter value is
-- outside their range. Only the `PrimaryKey` section is read: other indexes can be added by random settings.
SELECT 'granules, no mode', arrayFirst(x -> x LIKE 'Granules: %/%', arraySlice(lines, indexOf(lines, 'PrimaryKey')))
FROM (SELECT groupArray(trim(explain)) AS lines FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_week_key WHERE toDayOfWeek(d) = 1
    SETTINGS use_query_condition_cache = 0, enable_parallel_replicas = 0));
SELECT 'granules, mode 3', arrayFirst(x -> x LIKE 'Granules: %/%', arraySlice(lines, indexOf(lines, 'PrimaryKey')))
FROM (SELECT groupArray(trim(explain)) AS lines FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_week_key WHERE toDayOfWeek(d, 3) = 1
    SETTINGS use_query_condition_cache = 0, enable_parallel_replicas = 0));
SELECT 'granules, mode 1', arrayFirst(x -> x LIKE 'Granules: %/%', arraySlice(lines, indexOf(lines, 'PrimaryKey')))
FROM (SELECT groupArray(trim(explain)) AS lines FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_week_key WHERE toDayOfWeek(d, 1) = 5
    SETTINGS use_query_condition_cache = 0, enable_parallel_replicas = 0));

DROP TABLE t_week_key;
