-- `FORMAT Hash` checks the values for negative zeros, but a column whose type has no floating point values,
-- such as `LowCardinality(String)`, cannot have them, so it must not be expanded from its dictionary for that.
-- The expanded column here would take about 500 MiB, while the dictionary has only two values.

DROP TABLE IF EXISTS t_hash_wide_lc;
CREATE TABLE t_hash_wide_lc (s LowCardinality(String)) ENGINE = Memory;
INSERT INTO t_hash_wide_lc
SELECT toLowCardinality(concat(toString(number % 2), repeat('x', 8000)))
FROM numbers(65536)
SETTINGS max_threads = 1, max_block_size = 65536;

SELECT s FROM t_hash_wide_lc SETTINGS max_threads = 1, max_block_size = 65536, max_memory_usage = 100000000 FORMAT Null;
-- The fingerprint is pinned, so that it is the same as before: the dictionary values are hashed as they are.
SELECT s FROM t_hash_wide_lc SETTINGS max_threads = 1, max_block_size = 65536, max_memory_usage = 100000000 FORMAT Hash;

-- The same for a `LowCardinality(String)` nested in a composite column with a floating point value.
SELECT tuple(s, toFloat64(1)) FROM t_hash_wide_lc SETTINGS max_threads = 1, max_block_size = 65536, max_memory_usage = 100000000 FORMAT Null;
SELECT tuple(s, toFloat64(1)) FROM t_hash_wide_lc SETTINGS max_threads = 1, max_block_size = 65536, max_memory_usage = 100000000 FORMAT Hash;
-- And with a negative zero in it, which still tells the two zeros apart without expanding the strings.
SELECT tuple(s, toFloat64(-0.)) FROM t_hash_wide_lc SETTINGS max_threads = 1, max_block_size = 65536, max_memory_usage = 100000000 FORMAT Hash;
SELECT tuple(s, toFloat64(0.)) FROM t_hash_wide_lc SETTINGS max_threads = 1, max_block_size = 65536, max_memory_usage = 100000000 FORMAT Hash;

-- The same for a `LowCardinality(Float64)` column whose dictionary has no negative zeros.
SET allow_suspicious_low_cardinality_types = 1;
SELECT toLowCardinality(toFloat64(number % 3)) FROM numbers(10) FORMAT Hash;
SELECT toFloat64(number % 3) FROM numbers(10) FORMAT Hash;

DROP TABLE t_hash_wide_lc;
