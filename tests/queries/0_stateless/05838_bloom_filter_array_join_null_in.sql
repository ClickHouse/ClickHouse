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

-- A Dynamic cast can produce different strings at insert and query time.
INSERT INTO bloom_array_join_null_in
SELECT 6, '{"tags":["other"]}'::JSON, '{"tags":[true]}'::JSON
SETTINGS bool_true_representation = 'true';

SELECT arraySort(groupArray(id))
FROM bloom_array_join_null_in
WHERE arrayJoin(emptyArrayToSingle(dynamic_doc.tags::Array(String)))::Nullable(String)
    GLOBAL IN (SELECT 'yes'::Nullable(String))
SETTINGS transform_null_in = 1, bool_true_representation = 'yes', use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;

DROP TABLE bloom_array_join_lookup;
DROP TABLE bloom_array_join_null_in;
