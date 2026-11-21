-- A `DateTime` tuple element without a time zone in its type keeps the zone the table's types were built with,
-- while comparing the tuple with a tuple constant parses a text element in the session's zone. Key analysis
-- must read the text elements the same way before it pushes the constant through a key expression over the
-- tuple. `count()` goes through key analysis, `countIf` reads every row; the two must agree.

SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;
SET explain_query_plan_default = 'legacy';  -- for the Granules assertion
SET parallel_replicas_local_plan = 1;       -- for explain with indexes
SET session_timezone = 'UTC';               -- the zone the key types below are built with
SET allow_suspicious_variant_types = 1;     -- for Variant(Date, DateTime('UTC'))

DROP TABLE IF EXISTS t_hash, t_element, t_nested, t_dt64, t_skip, t_explicit, t_named, t_named_nested, t_erased;

CREATE TABLE t_hash (t Tuple(DateTime, UInt8)) ENGINE = MergeTree ORDER BY cityHash64(t) SETTINGS index_granularity = 1;
CREATE TABLE t_named (t Tuple(d DateTime, n UInt8)) ENGINE = MergeTree ORDER BY cityHash64(t) SETTINGS index_granularity = 1;
CREATE TABLE t_named_nested (t Tuple(k UInt8, p Tuple(d DateTime, n UInt8))) ENGINE = MergeTree ORDER BY cityHash64(t) SETTINGS index_granularity = 1;
CREATE TABLE t_element (t Tuple(DateTime, UInt8)) ENGINE = MergeTree ORDER BY t.1 SETTINGS index_granularity = 1;
CREATE TABLE t_nested (t Tuple(Tuple(DateTime, UInt8), UInt8)) ENGINE = MergeTree ORDER BY cityHash64(t) SETTINGS index_granularity = 1;
CREATE TABLE t_dt64 (t Tuple(DateTime64(3), UInt8)) ENGINE = MergeTree ORDER BY cityHash64(t) SETTINGS index_granularity = 1;
CREATE TABLE t_skip (k UInt64, t Tuple(DateTime, UInt8), INDEX i cityHash64(t) TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
CREATE TABLE t_explicit (t Tuple(DateTime('UTC'), UInt8)) ENGINE = MergeTree ORDER BY cityHash64(t) SETTINGS index_granularity = 1;
CREATE TABLE t_erased (t Tuple(DateTime, DateTime('UTC'))) ENGINE = MergeTree ORDER BY cityHash64(t) SETTINGS index_granularity = 1;

-- Rows 0-7 hold `2026-01-01 12:00:00` in America/New_York, rows 8-15 later instants.
INSERT INTO t_hash SELECT (toDateTime('2026-01-01 12:00:00', 'America/New_York') + if(number < 8, 0, number * 3600), 1) FROM numbers(16);
INSERT INTO t_element SELECT (toDateTime('2026-01-01 12:00:00', 'America/New_York') + if(number < 8, 0, number * 3600), 1) FROM numbers(16);
INSERT INTO t_nested SELECT ((toDateTime('2026-01-01 12:00:00', 'America/New_York') + if(number < 8, 0, number * 3600), 1), 2) FROM numbers(16);
INSERT INTO t_dt64 SELECT (toDateTime64('2026-01-01 12:00:00', 3, 'America/New_York') + if(number < 8, 0, number * 3600), 1) FROM numbers(16);
INSERT INTO t_skip SELECT number, (toDateTime('2026-01-01 12:00:00', 'America/New_York') + if(number < 8, 0, number * 3600), 1) FROM numbers(16);
INSERT INTO t_explicit SELECT (toDateTime('2026-01-01 12:00:00', 'UTC') + if(number < 8, 0, number * 3600), 1) FROM numbers(16);
INSERT INTO t_named SELECT (toDateTime('2026-01-01 12:00:00', 'America/New_York') + if(number < 8, 0, number * 3600), 1) FROM numbers(16);
INSERT INTO t_named_nested SELECT (2, (toDateTime('2026-01-01 12:00:00', 'America/New_York') + if(number < 8, 0, number * 3600), 1)) FROM numbers(16);
INSERT INTO t_erased SELECT (toDateTime('2026-01-01 12:00:00', 'America/New_York') + if(number < 8, 0, number * 3600), toDateTime('2026-01-02 00:00:00', 'UTC')) FROM numbers(16);

SET session_timezone = 'America/New_York';

SELECT count() FROM t_hash WHERE t = ('2026-01-01 12:00:00', toUInt8(1));
SELECT countIf(t = ('2026-01-01 12:00:00', toUInt8(1))) FROM t_hash;
SELECT count() FROM t_element WHERE t = ('2026-01-01 12:00:00', toUInt8(1));
SELECT countIf(t = ('2026-01-01 12:00:00', toUInt8(1))) FROM t_element;
SELECT count() FROM t_nested WHERE t = (('2026-01-01 12:00:00', toUInt8(1)), toUInt8(2));
SELECT countIf(t = (('2026-01-01 12:00:00', toUInt8(1)), toUInt8(2))) FROM t_nested;
SELECT count() FROM t_dt64 WHERE t = ('2026-01-01 12:00:00', toUInt8(1));
SELECT countIf(t = ('2026-01-01 12:00:00', toUInt8(1))) FROM t_dt64;
SELECT count() FROM t_skip WHERE t = ('2026-01-01 12:00:00', toUInt8(1));
SELECT countIf(t = ('2026-01-01 12:00:00', toUInt8(1))) FROM t_skip;

-- An element with an explicit time zone is read in that zone by both sides.
SELECT count() FROM t_explicit WHERE t = ('2026-01-01 12:00:00', toUInt8(1));
SELECT countIf(t = ('2026-01-01 12:00:00', toUInt8(1))) FROM t_explicit;

-- The comparison pairs the elements of named tuples by position, whatever their names.
SELECT count() FROM t_named WHERE t = CAST(('2026-01-01 12:00:00', 1), 'Tuple(d String, n UInt8)');
SELECT countIf(t = CAST(('2026-01-01 12:00:00', 1), 'Tuple(d String, n UInt8)')) FROM t_named;
SELECT count() FROM t_named WHERE t = CAST(('2026-01-01 12:00:00', 1), 'Tuple(n String, d UInt8)');
SELECT countIf(t = CAST(('2026-01-01 12:00:00', 1), 'Tuple(n String, d UInt8)')) FROM t_named;
SELECT count() FROM t_named_nested WHERE t = CAST((2, ('2026-01-01 12:00:00', 1)), 'Tuple(UInt8, Tuple(n String, d UInt8))');
SELECT countIf(t = CAST((2, ('2026-01-01 12:00:00', 1)), 'Tuple(UInt8, Tuple(n String, d UInt8))')) FROM t_named_nested;

-- The other elements keep their values, including the active type of a `Dynamic` or `Variant`.
SELECT count() FROM t_erased WHERE t = ('2026-01-01 12:00:00', CAST(toDate('2026-01-02'), 'Dynamic'));
SELECT countIf(t = ('2026-01-01 12:00:00', CAST(toDate('2026-01-02'), 'Dynamic'))) FROM t_erased;
SELECT count() FROM t_erased WHERE t = ('2026-01-01 12:00:00', CAST(toDateTime('2026-01-02 00:00:00', 'UTC'), 'Variant(Date, DateTime(\'UTC\'))'));
SELECT countIf(t = ('2026-01-01 12:00:00', CAST(toDateTime('2026-01-02 00:00:00', 'UTC'), 'Variant(Date, DateTime(\'UTC\'))'))) FROM t_erased;

-- The key still prunes the rows holding other instants.
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_hash WHERE t = ('2026-01-01 12:00:00', toUInt8(1)))
WHERE explain ILIKE '%Granules: %/16' AND explain NOT ILIKE '%Granules: 16/16';

DROP TABLE t_hash, t_element, t_nested, t_dt64, t_skip, t_explicit, t_named, t_named_nested, t_erased;
