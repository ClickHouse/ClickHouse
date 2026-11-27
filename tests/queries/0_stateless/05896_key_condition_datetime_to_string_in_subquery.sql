-- A one-argument `toString` over an explicit fixed-offset `DateTime` can use a DateTime set only with simple output.

DROP TABLE IF EXISTS t_utc;
DROP TABLE IF EXISTS t_fixed_offset;
DROP TABLE IF EXISTS t_dst;

CREATE TABLE t_utc (k DateTime('UTC')) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_utc SELECT toDateTime(number, 'UTC') FROM numbers(100);

SELECT 'UTC primary key condition', count() FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM t_utc WHERE toString(k) IN (SELECT toDateTime(42, 'UTC'))
    SETTINGS date_time_output_format = 'simple', cast_string_to_date_time_mode = 'basic',
        use_statistics_for_part_pruning = 0, use_query_condition_cache = 0
)
WHERE explain ILIKE '%Condition: (toString(k) in %';

SELECT 'UTC match', count(), (SELECT countIf(toString(k) IN (SELECT toDateTime(42, 'UTC'))) FROM t_utc)
FROM t_utc
WHERE toString(k) IN (SELECT toDateTime(42, 'UTC'))
SETTINGS date_time_output_format = 'simple', cast_string_to_date_time_mode = 'basic',
    use_statistics_for_part_pruning = 0, use_query_condition_cache = 0;

SELECT k FROM t_utc
WHERE toString(k) IN (SELECT toDateTime(42, 'UTC'))
SETTINGS max_rows_to_read = 40, date_time_output_format = 'simple', cast_string_to_date_time_mode = 'basic',
    use_statistics_for_part_pruning = 0, use_query_condition_cache = 0;

CREATE TABLE t_fixed_offset (k DateTime('Etc/GMT-3')) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_fixed_offset SELECT toDateTime(number, 'Etc/GMT-3') FROM numbers(100);

SELECT 'fixed-offset primary key condition', count() FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM t_fixed_offset WHERE toString(k) IN (SELECT toDateTime(42, 'Etc/GMT-3'))
    SETTINGS date_time_output_format = 'simple', cast_string_to_date_time_mode = 'basic',
        use_statistics_for_part_pruning = 0, use_query_condition_cache = 0
)
WHERE explain ILIKE '%Condition: (toString(k) in %';

SELECT 'fixed-offset match', count() FROM t_fixed_offset
WHERE toString(k) IN (SELECT toDateTime(42, 'Etc/GMT-3'))
SETTINGS max_rows_to_read = 40, date_time_output_format = 'simple', cast_string_to_date_time_mode = 'basic',
    use_statistics_for_part_pruning = 0, use_query_condition_cache = 0;

-- The DateTime timezone participates in the `Set` cast: this non-UTC subquery matches the UTC key's text, but stays unindexed.
SELECT 'non-UTC primary key condition', count() FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM t_utc WHERE toString(k) IN (SELECT toDateTime(18042, 'America/New_York'))
    SETTINGS date_time_output_format = 'simple', use_statistics_for_part_pruning = 0, use_query_condition_cache = 0
)
WHERE explain ILIKE '%Condition: (toString(k) in %';

SELECT 'non-UTC match', count() FROM t_utc
WHERE toString(k) IN (SELECT toDateTime(18042, 'America/New_York'))
SETTINGS date_time_output_format = 'simple', use_statistics_for_part_pruning = 0, use_query_condition_cache = 0;

-- An alternate output format must not use the simple DateTime-to-String set conversion.
SELECT 'ISO primary key condition', count() FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM t_utc WHERE toString(k) IN (SELECT toDateTime(42, 'UTC'))
    SETTINGS date_time_output_format = 'iso', cast_string_to_date_time_mode = 'best_effort',
        use_statistics_for_part_pruning = 0, use_query_condition_cache = 0
)
WHERE explain ILIKE '%Condition: (toString(k) in %';

SELECT 'ISO match', count() FROM t_utc
WHERE toString(k) IN (SELECT toDateTime(42, 'UTC'))
SETTINGS date_time_output_format = 'iso', cast_string_to_date_time_mode = 'best_effort',
    use_statistics_for_part_pruning = 0, use_query_condition_cache = 0;

-- The two instants in the fall-back hour stringify identically; a DST-zone transform cannot be used as a key condition.
CREATE TABLE t_dst (k DateTime('America/New_York')) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_dst SELECT toDateTime(1636263000 + number * 3600, 'America/New_York') FROM numbers(2);

SELECT 'DST repeated-hour matches', count(), uniqExact(toString(k)) FROM t_dst
WHERE toString(k) IN (SELECT toDateTime(1636263000, 'America/New_York'))
SETTINGS date_time_output_format = 'simple', use_statistics_for_part_pruning = 0, use_query_condition_cache = 0;

DROP TABLE t_utc;
DROP TABLE t_fixed_offset;
DROP TABLE t_dst;
