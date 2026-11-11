-- A string constant that does not parse as the key type cannot bound an index range,
-- but the comparison itself accepts it, so index analysis must not fail the query.

DROP TABLE IF EXISTS t_unconvertible_pk;
DROP TABLE IF EXISTS t_unconvertible_stats;

CREATE TABLE t_unconvertible_pk (t DateTime64(3, 'UTC'), id UInt32) ENGINE = MergeTree ORDER BY t SETTINGS index_granularity = 1;
INSERT INTO t_unconvertible_pk VALUES ('2026-07-16 12:30:45.123', 1), ('2026-07-17 00:00:00', 2);

SELECT count() FROM t_unconvertible_pk WHERE t = '2026-07-16T12:30:45.123Z';
SELECT count() FROM t_unconvertible_pk WHERE t < '2026-07-16T12:30:46Z';

CREATE TABLE t_unconvertible_stats (id UInt32, t DateTime64(3, 'UTC') STATISTICS(basic)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_unconvertible_stats VALUES (1, '2026-07-16 12:30:45.123');
INSERT INTO t_unconvertible_stats VALUES (2, '2026-07-17 00:00:00');

SELECT count() FROM t_unconvertible_stats WHERE t = '2026-07-16T12:30:45.123Z'
SETTINGS use_statistics_for_part_pruning = 1, min_marks_per_part_to_load_statistics = 0;
SELECT count() FROM t_unconvertible_stats WHERE t = '02/01/2024 03:04:05'
SETTINGS use_statistics_for_part_pruning = 1, min_marks_per_part_to_load_statistics = 0, cast_string_to_date_time_mode = 'best_effort_us';

DROP TABLE t_unconvertible_pk;
DROP TABLE t_unconvertible_stats;
