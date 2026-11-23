DROP TABLE IF EXISTS bloom_array_join_null_in;
DROP TABLE IF EXISTS bloom_array_join_lookup;

CREATE TABLE bloom_array_join_null_in
(
    id UInt64,
    typed_doc JSON(tags Array(String)),
    dynamic_doc JSON,
    INDEX idx_typed_tags typed_doc.tags TYPE bloom_filter(0.0001) GRANULARITY 1,
    INDEX idx_dynamic_tags dynamic_doc.tags::Array(String) TYPE bloom_filter(0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO bloom_array_join_null_in
SELECT id, doc::JSON, doc::JSON
FROM
(
    SELECT id, toJSONString(map('tags', tags)) AS doc
    FROM values('id UInt64, tags Array(String)', (1, ['needle', 'needle']), (2, ['other']), (3, []), (4, ['']), (5, ['tail']))
);

CREATE TABLE bloom_array_join_lookup (kind UInt8, v Nullable(String)) ENGINE = Memory;
INSERT INTO bloom_array_join_lookup VALUES (1, 'needle'), (2, ''), (3, NULL), (3, 'needle');

-- IN and GLOBAL IN with transform_null_in can use the index on the typed path.
SELECT countIf(explain LIKE '%Name: idx_typed_tags%') > 0, countIf(match(explain, 'Granules: [1-4]/5')) > 0
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM bloom_array_join_null_in
    WHERE arrayJoin(emptyArrayToSingle(typed_doc.tags))::Nullable(String)
        IN (SELECT v FROM bloom_array_join_lookup WHERE kind = 1)
    SETTINGS transform_null_in = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0
);

SELECT countIf(explain LIKE '%Name: idx_typed_tags%') > 0, countIf(match(explain, 'Granules: [1-4]/5')) > 0
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM bloom_array_join_null_in
    WHERE arrayJoin(typed_doc.tags)::Nullable(String)
        GLOBAL IN (SELECT v FROM bloom_array_join_lookup WHERE kind = 1)
    SETTINGS transform_null_in = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0
);

-- Preserve duplicate matches, the padded empty string, and matches in a set containing NULL.
SELECT arraySort(groupArray(id))
FROM bloom_array_join_null_in
WHERE arrayJoin(emptyArrayToSingle(typed_doc.tags))::Nullable(String)
    IN (SELECT v FROM bloom_array_join_lookup WHERE kind = 1)
SETTINGS use_skip_indexes = 1, transform_null_in = 1;

SELECT arraySort(groupArray(id))
FROM bloom_array_join_null_in
WHERE arrayJoin(emptyArrayToSingle(typed_doc.tags))::Nullable(String)
    GLOBAL IN (SELECT v FROM bloom_array_join_lookup WHERE kind = 2)
SETTINGS use_skip_indexes = 1, transform_null_in = 1;

SELECT arraySort(groupArray(id))
FROM bloom_array_join_null_in
WHERE arrayJoin(emptyArrayToSingle(typed_doc.tags))::Nullable(String)
    IN (SELECT v FROM bloom_array_join_lookup WHERE kind = 3)
SETTINGS use_skip_indexes = 1, transform_null_in = 1;

-- The round trip through Dynamic does not change the values of the typed path.
SELECT countIf(explain LIKE '%Name: idx_typed_tags%') > 0, countIf(match(explain, 'Granules: [1-4]/5')) > 0
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM bloom_array_join_null_in
    WHERE arrayJoin(emptyArrayToSingle(typed_doc.tags::Dynamic::Array(String)))::Nullable(String)
        GLOBAL IN (SELECT v FROM bloom_array_join_lookup WHERE kind = 1)
    SETTINGS transform_null_in = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0
);

SELECT arraySort(groupArray(id))
FROM bloom_array_join_null_in
WHERE arrayJoin(emptyArrayToSingle(typed_doc.tags::Dynamic::Array(String)))::Nullable(String)
    GLOBAL IN (SELECT v FROM bloom_array_join_lookup WHERE kind = 1)
SETTINGS use_skip_indexes = 1, transform_null_in = 1;

SELECT arraySort(groupArray(id))
FROM bloom_array_join_null_in
WHERE arrayJoin(emptyArrayToSingle(typed_doc.tags::Dynamic::Array(String)))::Nullable(String)
    GLOBAL IN (SELECT v FROM bloom_array_join_lookup WHERE kind = 2)
SETTINGS use_skip_indexes = 1, transform_null_in = 1;

-- An index on an expression of the untyped path.
SELECT countIf(explain LIKE '%Name: idx_dynamic_tags%') > 0, countIf(match(explain, 'Granules: [1-4]/5')) > 0
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM bloom_array_join_null_in
    WHERE arrayJoin(emptyArrayToSingle(dynamic_doc.tags::Dynamic::Array(String)))::Nullable(String)
        IN (SELECT v FROM bloom_array_join_lookup WHERE kind = 1)
    SETTINGS transform_null_in = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0
);

SELECT arraySort(groupArray(id))
FROM bloom_array_join_null_in
WHERE arrayJoin(emptyArrayToSingle(dynamic_doc.tags::Dynamic::Array(String)))::Nullable(String)
    IN (SELECT v FROM bloom_array_join_lookup WHERE kind = 2)
SETTINGS use_skip_indexes = 1, transform_null_in = 1;

DROP TABLE bloom_array_join_lookup;
DROP TABLE bloom_array_join_null_in;

DROP TABLE IF EXISTS bloom_array_join_types;

CREATE TABLE bloom_array_join_types
(
    id UInt64,
    nums Array(UInt64),
    name String,
    nint Nullable(Int64),
    INDEX idx_nums nums TYPE bloom_filter(0.0001) GRANULARITY 1,
    INDEX idx_name name TYPE bloom_filter(0.0001) GRANULARITY 1,
    INDEX idx_nint nint TYPE bloom_filter(0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

-- The NULL of row 2 keeps 2 in its nested column, which is what the index hashes.
INSERT INTO bloom_array_join_types
SELECT id, nums, name, if(id = 2, NULL, id)
FROM values('id UInt64, nums Array(UInt64), name String', (1, [10, 10], '1.50'), (2, [20], 'b'), (3, [], 'c'), (4, [0], 'd'), (5, [50], 'e'));

-- Other element types work the same way.
SELECT countIf(explain LIKE '%Name: idx_nums%') > 0, countIf(match(explain, 'Granules: [1-4]/5')) > 0
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM bloom_array_join_types
    WHERE arrayJoin(nums::Dynamic::Array(UInt64)) IN (SELECT 10::Nullable(UInt64))
    SETTINGS transform_null_in = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0
);

SELECT arraySort(groupArray(id))
FROM bloom_array_join_types
WHERE arrayJoin(emptyArrayToSingle(nums::Dynamic::Array(UInt64))) IN (0, 10)
SETTINGS use_skip_indexes = 1, transform_null_in = 1;

-- Not lossless: Dynamic may parse a top-level String, and turns NULL into the default of a non-Nullable type.
SELECT arraySort(groupArray(id))
FROM bloom_array_join_types
WHERE name::Dynamic::String = '1.5'
SETTINGS use_skip_indexes = 1, cast_string_to_dynamic_use_inference = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0;

SELECT arraySort(groupArray(id))
FROM bloom_array_join_types
WHERE nint::Dynamic::Int64 = 0
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0;

DROP TABLE bloom_array_join_types;

DROP TABLE IF EXISTS bloom_array_join_padding;

CREATE TABLE bloom_array_join_padding
(
    id UInt64,
    tags Array(LowCardinality(Nullable(String))),
    INDEX idx_tags tags TYPE bloom_filter(0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO bloom_array_join_padding VALUES (1, []), (2, ['other']);

-- A conversion under emptyArrayToSingle changes the padded element from NULL to ''.
SELECT groupArray(id)
FROM bloom_array_join_padding
WHERE arrayJoin(emptyArrayToSingle(CAST(tags, 'Array(String)'))) = ''
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0;

SELECT groupArray(id)
FROM bloom_array_join_padding
WHERE arrayJoin(emptyArrayToSingle(CAST(tags, 'Array(String)'))) IN ('')
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0;

DROP TABLE bloom_array_join_padding;

DROP TABLE IF EXISTS bloom_array_join_tuple;

CREATE TABLE bloom_array_join_tuple
(
    id UInt64,
    tags Array(String),
    INDEX idx_tags tags TYPE bloom_filter(0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO bloom_array_join_tuple VALUES (1, ['target']), (2, ['other']);

-- A NULL inside a tuple member of the set matches no element.
SELECT groupArray(id)
FROM bloom_array_join_tuple
WHERE (tuple(arrayJoin(tags)), id) IN (SELECT tuple(v), i FROM values('v Nullable(String), i UInt64', ('target', 1), (NULL, 2)))
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0;

SELECT groupArray(id)
FROM bloom_array_join_tuple
WHERE (tuple(arrayJoin(CAST(tags, 'Array(Nullable(String))'))), id) IN (SELECT tuple(v), i FROM values('v Nullable(String), i UInt64', ('target', 1), (NULL, 2)))
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0;

DROP TABLE bloom_array_join_tuple;
