-- Random settings limits: group_by_two_level_threshold=(10, None)
-- GROUP BY over a 4-byte LowCardinality key must not store the default value twice in the
-- result dictionary: once the block is squashed with default-key rows, the part is sorted wrong.

SET allow_suspicious_low_cardinality_types = 1;
SET max_threads = 1;

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;

CREATE TABLE src (a LowCardinality(UInt32)) ENGINE = Memory;
INSERT INTO src VALUES (0), (1), (2);
CREATE TABLE dst (a LowCardinality(UInt32), c UInt64) ENGINE = MergeTree ORDER BY (a, c) SETTINGS index_granularity = 1;

SELECT 'UInt32, ROLLUP';
INSERT INTO dst SELECT a, count() FROM src GROUP BY a WITH ROLLUP;
SELECT a, c FROM dst ORDER BY a, c SETTINGS optimize_read_in_order = 1;
SELECT count() FROM dst WHERE a = 0 AND c = 1;

SELECT 'UInt32, GROUPING SETS';
TRUNCATE TABLE dst;
INSERT INTO dst SELECT a, count() FROM src GROUP BY GROUPING SETS ((a), ());
SELECT a, c FROM dst ORDER BY a, c SETTINGS optimize_read_in_order = 1;
SELECT count() FROM dst WHERE a = 0 AND c = 1;

SELECT 'UInt32, ROLLUP, two-level';
TRUNCATE TABLE dst;
INSERT INTO dst SELECT a, count() FROM src GROUP BY a WITH ROLLUP SETTINGS group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1, max_bytes_before_external_group_by = 10000000000, max_bytes_ratio_before_external_group_by = 0;
SELECT a, c FROM dst ORDER BY a, c SETTINGS optimize_read_in_order = 1;
SELECT count() FROM dst WHERE a = 0 AND c = 1;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['AggregationConvertedToTwoLevel'] + ProfileEvents['AggregationHashTablesInitializedAsTwoLevel'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Insert' AND query LIKE '%group_by_two_level_threshold = 1%';

DROP TABLE src;
DROP TABLE dst;

SELECT 'DateTime, ROLLUP';
CREATE TABLE src (a LowCardinality(DateTime('UTC'))) ENGINE = Memory;
INSERT INTO src VALUES (toDateTime(0, 'UTC')), (toDateTime(1, 'UTC')), (toDateTime(2, 'UTC'));
CREATE TABLE dst (a LowCardinality(DateTime('UTC')), c UInt64) ENGINE = MergeTree ORDER BY (a, c) SETTINGS index_granularity = 1;
INSERT INTO dst SELECT a, count() FROM src GROUP BY a WITH ROLLUP;
SELECT a, c FROM dst ORDER BY a, c SETTINGS optimize_read_in_order = 1;
SELECT count() FROM dst WHERE a = toDateTime(0, 'UTC') AND c = 1;
DROP TABLE src;
DROP TABLE dst;

SELECT 'Nullable(UInt32), UNION ALL';
CREATE TABLE src (a LowCardinality(Nullable(UInt32))) ENGINE = Memory;
INSERT INTO src VALUES (0), (1), (2);
CREATE TABLE dst (a LowCardinality(Nullable(UInt32)), c UInt64) ENGINE = MergeTree ORDER BY (a, c) SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO dst SELECT a, count() FROM src GROUP BY a UNION ALL SELECT toLowCardinality(toNullable(toUInt32(0))), 5;
SELECT a, c FROM dst ORDER BY a, c SETTINGS optimize_read_in_order = 1;
SELECT count() FROM dst WHERE a = 0 AND c = 1;
DROP TABLE src;
DROP TABLE dst;
