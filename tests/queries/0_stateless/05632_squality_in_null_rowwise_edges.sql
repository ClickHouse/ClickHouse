SET in_null_as_unknown = 1;

-- Failed conversion is not a source `NULL`.
SELECT toUInt8(1) IN ('256'), toUInt8(1) NOT IN ('256');
SELECT toUInt8(1) IN materialize('256'), toUInt8(1) NOT IN materialize('256');
SELECT number FROM numbers(3) WHERE toUInt8(number) NOT IN materialize('256') ORDER BY number;
SELECT CAST(NULL AS Nullable(UInt8)) IN materialize('256');
SELECT toUInt8(1) IN materialize(CAST(NULL AS Nullable(String)));

-- A genuine `NULL` member is excluded before converting complex values to an array.
SELECT [1] IN ([number + 1], NULL), [1] NOT IN ([number + 1], NULL) FROM numbers(1);
SELECT [2] IN ([number + 1], NULL), [2] NOT IN ([number + 1], NULL) FROM numbers(1);
SELECT map('a', toUInt64(1)) IN (map('a', number + 1), NULL),
    map('a', toUInt64(1)) NOT IN (map('a', number + 1), NULL) FROM numbers(1);
SELECT map('a', toUInt64(2)) IN (map('a', number + 1), NULL),
    map('a', toUInt64(2)) NOT IN (map('a', number + 1), NULL) FROM numbers(1);

-- A nonempty null-only source stays nonempty after removing matchable values.
SELECT toNullable(1) IN (NULL), toNullable(1) NOT IN (NULL),
    toNullable(1) IN (materialize(NULL), NULL),
    toNullable(1) NOT IN (materialize(NULL), NULL);
SELECT toUInt8(1) IN (materialize(NULL), NULL),
    toUInt8(1) NOT IN (materialize(NULL), NULL);
SELECT k FROM VALUES('k Nullable(UInt8)', (NULL), (1))
WHERE k NOT IN (materialize(NULL), NULL);

-- Failed casts are excluded from membership without becoming source `NULL`s.
SELECT toUInt8(1) IN ('256', '257'), toUInt8(1) NOT IN ('256', '257');
SELECT toUInt8(1) IN (materialize('256'), '257'),
    toUInt8(1) NOT IN (materialize('256'), '257');
SELECT number FROM numbers(3)
WHERE toUInt8(number) NOT IN (materialize('256'), '257') ORDER BY number;

CREATE TABLE squality_in_null_internal_join (k Tuple(UInt64, UInt64), v UInt64)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO squality_in_null_internal_join
SELECT tuple(number, toUInt64(1)), number FROM numbers(10000);

-- Internal runtime filters compare join keys under the stored-key rules.
SELECT sum(t.v), sum(r.r)
FROM squality_in_null_internal_join AS t INNER JOIN
    (SELECT tuple(number, toUInt64(1)) AS k, number AS r FROM numbers(2)) AS r
USING k
SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0,
    join_algorithm = 'hash', query_plan_join_swap_table = 'false', max_threads = 1;

DROP TABLE squality_in_null_internal_join;

CREATE TABLE squality_in_null_internal_final (k1 UInt64, k2 UInt64, v UInt64)
ENGINE = ReplacingMergeTree ORDER BY (k1, k2);
SYSTEM STOP MERGES squality_in_null_internal_final;
INSERT INTO squality_in_null_internal_final VALUES (1, 1, 10), (2, 1, 20);
INSERT INTO squality_in_null_internal_final VALUES (1, 1, 30), (3, 1, 40);

-- Lazy `FINAL` also builds an internal key-membership predicate.
SELECT sum(v) FROM squality_in_null_internal_final FINAL WHERE k1 > 0
SETTINGS query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0;

DROP TABLE squality_in_null_internal_final;
