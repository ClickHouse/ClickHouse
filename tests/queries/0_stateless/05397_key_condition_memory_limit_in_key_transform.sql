-- Index analysis pushes the constant of `s = ...` through every key expression computable from `s`. A memory
-- limit hit there is an outer guard of the query, so the query aborts instead of going on without that atom.
-- Each case first runs the same query within the same limit without that work.

SET analyze_index_with_multiple_key_columns_per_condition = 1;

-- The key expression `length(repeat(s, 1000000))` turns a 100-byte constant into a 100 MB string. With one
-- key column per condition, the analysis uses only `s`.
CREATE TABLE t_dag (s String) ENGINE = MergeTree ORDER BY (s, length(repeat(s, 1000000)));
INSERT INTO t_dag VALUES ('');

SELECT count() FROM t_dag WHERE s = repeat('a', 100)
SETTINGS max_memory_usage = 30000000, analyze_index_with_multiple_key_columns_per_condition = 0;
SELECT count() FROM t_dag WHERE s = repeat('a', 100) SETTINGS max_memory_usage = 30000000; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- Before the transform, the constant is cast into the type the key expression reads: padding `'abc'` to
-- `FixedString(16000000)` takes 16 MB. `EXPLAIN` keeps the 16 MB value of the table out of the query, and
-- `use_skip_indexes = 0` keeps the analysis to the primary key.
SET allow_suspicious_fixed_string_types = 1;
CREATE TABLE t_cast (s FixedString(16000000)) ENGINE = MergeTree ORDER BY cityHash64(s);
INSERT INTO t_cast VALUES ('x');

SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cast WHERE s = 'abc')
SETTINGS max_memory_usage = 12000000, use_primary_key = 0, use_skip_indexes = 0;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cast WHERE s = 'abc')
SETTINGS max_memory_usage = 12000000, use_skip_indexes = 0; -- { serverError MEMORY_LIMIT_EXCEEDED }
