SET in_null_as_unknown = 1;

DROP TABLE IF EXISTS test_in_null_key_condition;
CREATE TABLE test_in_null_key_condition (x UInt64)
ENGINE = MergeTree
ORDER BY x
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0, auto_statistics_types = '';

INSERT INTO test_in_null_key_condition SELECT number FROM numbers(6);

SELECT count()
FROM test_in_null_key_condition
WHERE x NOT IN (SELECT value FROM VALUES('value Nullable(UInt64)', (1), (NULL)))
SETTINGS use_primary_key = 0, optimize_use_implicit_projections = 0, optimize_trivial_count_query = 0;

SELECT count()
FROM test_in_null_key_condition
WHERE x NOT IN (SELECT value FROM VALUES('value Nullable(UInt64)', (1), (NULL)))
SETTINGS use_primary_key = 1, optimize_use_implicit_projections = 0, optimize_trivial_count_query = 0;

SELECT count()
FROM test_in_null_key_condition
WHERE x NOT IN (SELECT value FROM VALUES('value Nullable(UInt64)', (1), (NULL)))
SETTINGS use_primary_key = 1, optimize_use_implicit_projections = 1, optimize_trivial_count_query = 0;

SELECT count()
FROM test_in_null_key_condition
WHERE x IN (SELECT value FROM VALUES('value Nullable(UInt64)', (1), (NULL)))
SETTINGS use_primary_key = 1, optimize_use_implicit_projections = 1, optimize_trivial_count_query = 0;

DROP TABLE test_in_null_key_condition;
