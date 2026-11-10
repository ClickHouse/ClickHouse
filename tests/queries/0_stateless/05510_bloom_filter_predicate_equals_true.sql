-- A `bloom_filter` index must be used for a predicate compared to true, e.g. `has(arr, x) = 1`,
-- just like for the bare predicate. https://github.com/ClickHouse/ClickHouse/issues/79712

SET explain_query_plan_default = 'legacy';
SET use_skip_indexes = 1;
SET use_query_condition_cache = 0;
SET use_skip_indexes_on_data_read = 0;

DROP TABLE IF EXISTS peoples;

CREATE TABLE peoples
(
    uid Int16,
    name String,
    friends Array(String),
    INDEX bf_friends friends TYPE bloom_filter GRANULARITY 1,
    INDEX bf_name name TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY uid SETTINGS index_granularity = 1, index_granularity_bytes = 10485760;

INSERT INTO peoples VALUES (1, 'Tom', ['Alice', 'Bob']);
INSERT INTO peoples VALUES (2, 'Jerry', ['Charlie']);

-- The `Granules:` line of the named index, or an empty string if the index is not used.
SELECT 'has', arrayFirst(x -> startsWith(x, 'Granules:'), arraySlice(e, indexOf(e, 'Name: bf_friends')))
FROM (SELECT groupArray(trimLeft(explain)) AS e FROM (EXPLAIN indexes = 1 SELECT * FROM peoples WHERE has(friends, 'Alice')));
SELECT 'has = 1', arrayFirst(x -> startsWith(x, 'Granules:'), arraySlice(e, indexOf(e, 'Name: bf_friends')))
FROM (SELECT groupArray(trimLeft(explain)) AS e FROM (EXPLAIN indexes = 1 SELECT * FROM peoples WHERE has(friends, 'Alice') = 1));
SELECT 'has = true', arrayFirst(x -> startsWith(x, 'Granules:'), arraySlice(e, indexOf(e, 'Name: bf_friends')))
FROM (SELECT groupArray(trimLeft(explain)) AS e FROM (EXPLAIN indexes = 1 SELECT * FROM peoples WHERE has(friends, 'Alice') = true));
SELECT 'has != 0', arrayFirst(x -> startsWith(x, 'Granules:'), arraySlice(e, indexOf(e, 'Name: bf_friends')))
FROM (SELECT groupArray(trimLeft(explain)) AS e FROM (EXPLAIN indexes = 1 SELECT * FROM peoples WHERE has(friends, 'Alice') != 0));
SELECT '1 = has', arrayFirst(x -> startsWith(x, 'Granules:'), arraySlice(e, indexOf(e, 'Name: bf_friends')))
FROM (SELECT groupArray(trimLeft(explain)) AS e FROM (EXPLAIN indexes = 1 SELECT * FROM peoples WHERE 1 = has(friends, 'Alice')));
SELECT 'hasAny = 1', arrayFirst(x -> startsWith(x, 'Granules:'), arraySlice(e, indexOf(e, 'Name: bf_friends')))
FROM (SELECT groupArray(trimLeft(explain)) AS e FROM (EXPLAIN indexes = 1 SELECT * FROM peoples WHERE hasAny(friends, ['Alice']) = 1));
SELECT 'equals', arrayFirst(x -> startsWith(x, 'Granules:'), arraySlice(e, indexOf(e, 'Name: bf_name')))
FROM (SELECT groupArray(trimLeft(explain)) AS e FROM (EXPLAIN indexes = 1 SELECT * FROM peoples WHERE name = 'Tom'));
SELECT 'equals = 1', arrayFirst(x -> startsWith(x, 'Granules:'), arraySlice(e, indexOf(e, 'Name: bf_name')))
FROM (SELECT groupArray(trimLeft(explain)) AS e FROM (EXPLAIN indexes = 1 SELECT * FROM peoples WHERE (name = 'Tom') = 1));

-- `= 0` selects the rows where the predicate is false, so the index cannot be used.
SELECT 'has = 0', arrayFirst(x -> startsWith(x, 'Granules:'), arraySlice(e, indexOf(e, 'Name: bf_friends')))
FROM (SELECT groupArray(trimLeft(explain)) AS e FROM (EXPLAIN indexes = 1 SELECT * FROM peoples WHERE has(friends, 'Alice') = 0));

-- Results are the same with and without the index.
SELECT 'has = 1', count() FROM peoples WHERE has(friends, 'Alice') = 1 SETTINGS use_skip_indexes = 1;
SELECT 'has = 1', count() FROM peoples WHERE has(friends, 'Alice') = 1 SETTINGS use_skip_indexes = 0;
SELECT 'has = true', count() FROM peoples WHERE has(friends, 'Alice') = true SETTINGS use_skip_indexes = 1;
SELECT 'has = true', count() FROM peoples WHERE has(friends, 'Alice') = true SETTINGS use_skip_indexes = 0;
SELECT 'has != 0', count() FROM peoples WHERE has(friends, 'Alice') != 0 SETTINGS use_skip_indexes = 1;
SELECT 'has != 0', count() FROM peoples WHERE has(friends, 'Alice') != 0 SETTINGS use_skip_indexes = 0;
SELECT 'hasAny = 1', count() FROM peoples WHERE hasAny(friends, ['Alice']) = 1 SETTINGS use_skip_indexes = 1;
SELECT 'hasAny = 1', count() FROM peoples WHERE hasAny(friends, ['Alice']) = 1 SETTINGS use_skip_indexes = 0;
SELECT 'equals = 1', count() FROM peoples WHERE (name = 'Tom') = 1 SETTINGS use_skip_indexes = 1;
SELECT 'equals = 1', count() FROM peoples WHERE (name = 'Tom') = 1 SETTINGS use_skip_indexes = 0;
SELECT 'has = 0', count() FROM peoples WHERE has(friends, 'Alice') = 0 SETTINGS use_skip_indexes = 1;
SELECT 'has = 0', count() FROM peoples WHERE has(friends, 'Alice') = 0 SETTINGS use_skip_indexes = 0;
SELECT 'absent = 1', count() FROM peoples WHERE has(friends, 'Zoe') = 1 SETTINGS use_skip_indexes = 1;
SELECT 'absent = 1', count() FROM peoples WHERE has(friends, 'Zoe') = 1 SETTINGS use_skip_indexes = 0;
SELECT 'not has = 1', count() FROM peoples WHERE (NOT has(friends, 'Alice')) = 1 SETTINGS use_skip_indexes = 1;
SELECT 'not has = 1', count() FROM peoples WHERE (NOT has(friends, 'Alice')) = 1 SETTINGS use_skip_indexes = 0;
SELECT 'has or = 1', count() FROM peoples WHERE (has(friends, 'Alice') OR has(friends, 'Charlie')) = 1 SETTINGS use_skip_indexes = 1;
SELECT 'has or = 1', count() FROM peoples WHERE (has(friends, 'Alice') OR has(friends, 'Charlie')) = 1 SETTINGS use_skip_indexes = 0;

DROP TABLE peoples;
