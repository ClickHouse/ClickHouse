DROP TABLE IF EXISTS t_bloom_json_array_join;
DROP TABLE IF EXISTS lookup;

CREATE TABLE t_bloom_json_array_join
(
    id UInt64,
    doc JSON(tags Array(String)),
    INDEX idx_doc_tags doc.tags TYPE bloom_filter(0.0001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_bloom_json_array_join
SELECT id, toJSONString(map('tags', tags))::JSON
FROM values('id UInt64, tags Array(String)',
    (1, ['needle', 'needle']),
    (2, ['other']),
    (3, []),
    (4, ['']),
    (5, ['tail']));

CREATE TABLE lookup
(
    kind UInt8,
    v Nullable(String)
)
ENGINE = Memory;

INSERT INTO lookup VALUES (1, 'needle'), (2, ''), (3, NULL), (3, 'needle');

-- The full typed-JSON query shape must use the named Bloom index and prune at least one of five granules.
SELECT
    countIf(explain LIKE '%Name: idx_doc_tags%') > 0,
    countIf(match(explain, 'Granules: [1-4]/5')) > 0
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM t_bloom_json_array_join
    WHERE arrayJoin(emptyArrayToSingle(doc.tags::Dynamic::Array(String)))::Nullable(String)
        GLOBAL IN (SELECT v::Nullable(String) FROM lookup WHERE kind = 1)
    SETTINGS transform_null_in = 1
);

-- A nullable set with a real match must not prune away rows because it also contains NULL.
SELECT
    countIf(explain LIKE '%Name: idx_doc_tags%') = 0
        OR countIf(match(explain, 'Granules: 5/5')) > 0
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM t_bloom_json_array_join
    WHERE arrayJoin(emptyArrayToSingle(doc.tags::Dynamic::Array(String)))::Nullable(String)
        GLOBAL IN (SELECT v::Nullable(String) FROM lookup WHERE kind = 3)
    SETTINGS transform_null_in = 1
);

-- Equality remains a separately supported positive case through the same JSON/ArrayJoin casts.
SELECT
    countIf(explain LIKE '%Name: idx_doc_tags%') > 0,
    countIf(match(explain, 'Granules: [1-4]/5')) > 0
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM t_bloom_json_array_join
    WHERE arrayJoin(emptyArrayToSingle(doc.tags::Dynamic::Array(String)))::Nullable(String) = 'needle'
);

-- Keep duplicate rows: the matching array contains the same value twice.
SELECT arraySort(groupArray(id))
FROM t_bloom_json_array_join
WHERE arrayJoin(emptyArrayToSingle(doc.tags::Dynamic::Array(String)))::Nullable(String)
    GLOBAL IN (SELECT v::Nullable(String) FROM lookup WHERE kind = 1)
SETTINGS use_skip_indexes = 1, transform_null_in = 1;

SELECT arraySort(groupArray(id))
FROM t_bloom_json_array_join
WHERE arrayJoin(emptyArrayToSingle(doc.tags::Dynamic::Array(String)))::Nullable(String)
    GLOBAL IN (SELECT v::Nullable(String) FROM lookup WHERE kind = 1)
SETTINGS use_skip_indexes = 0, transform_null_in = 1;

SELECT arraySort(groupArray(id))
FROM t_bloom_json_array_join
WHERE arrayJoin(emptyArrayToSingle(doc.tags::Dynamic::Array(String)))::Nullable(String)
    GLOBAL IN (SELECT v::Nullable(String) FROM lookup WHERE kind = 2)
SETTINGS use_skip_indexes = 1, transform_null_in = 1;

SELECT arraySort(groupArray(id))
FROM t_bloom_json_array_join
WHERE arrayJoin(emptyArrayToSingle(doc.tags::Dynamic::Array(String)))::Nullable(String)
    GLOBAL IN (SELECT v::Nullable(String) FROM lookup WHERE kind = 2)
SETTINGS use_skip_indexes = 0, transform_null_in = 1;

SELECT arraySort(groupArray(id))
FROM t_bloom_json_array_join
WHERE arrayJoin(emptyArrayToSingle(doc.tags::Dynamic::Array(String)))::Nullable(String)
    GLOBAL IN (SELECT v::Nullable(String) FROM lookup WHERE kind = 3)
SETTINGS use_skip_indexes = 1, transform_null_in = 1;

SELECT arraySort(groupArray(id))
FROM t_bloom_json_array_join
WHERE arrayJoin(emptyArrayToSingle(doc.tags::Dynamic::Array(String)))::Nullable(String)
    GLOBAL IN (SELECT v::Nullable(String) FROM lookup WHERE kind = 3)
SETTINGS use_skip_indexes = 0, transform_null_in = 1;

SELECT arraySort(groupArray(id))
FROM t_bloom_json_array_join
WHERE arrayJoin(emptyArrayToSingle(doc.tags::Dynamic::Array(String)))::Nullable(String) = 'needle'
SETTINGS use_skip_indexes = 1;

SELECT arraySort(groupArray(id))
FROM t_bloom_json_array_join
WHERE arrayJoin(emptyArrayToSingle(doc.tags::Dynamic::Array(String)))::Nullable(String) = 'needle'
SETTINGS use_skip_indexes = 0;

DROP TABLE lookup;
DROP TABLE t_bloom_json_array_join;
