DROP VIEW IF EXISTS issue126641_v;
DROP TABLE IF EXISTS issue126641_t;

CREATE TABLE issue126641_t (m Map(LowCardinality(String), String)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO issue126641_t SELECT map('k', toString(number), 'payload', repeat('x', 512)) FROM numbers(2000);
CREATE VIEW issue126641_v AS SELECT m FROM issue126641_t;

SELECT sum(length(m['k'])) FROM issue126641_v SETTINGS optimize_functions_to_subcolumns = 1;

SELECT sum(length(m['k'])) FROM (SELECT m FROM issue126641_t) SETTINGS optimize_functions_to_subcolumns = 1;

SELECT count() > 0
FROM (
    EXPLAIN PLAN actions = 1
    SELECT sum(length(m['k']))
    FROM issue126641_v
    SETTINGS optimize_functions_to_subcolumns = 1
)
WHERE explain ILIKE '%Output: m.key_k%';

SELECT count() > 0
FROM (
    EXPLAIN PLAN actions = 1
    SELECT sum(length(m['k']))
    FROM (SELECT m FROM issue126641_t)
    SETTINGS optimize_functions_to_subcolumns = 1, query_plan_remove_unused_columns = 1
)
WHERE explain ILIKE '%Output: m.key_k%';

SELECT count() > 0
FROM (
    EXPLAIN PLAN actions = 1
    SELECT sum(length(m.key_k))
    FROM issue126641_v
    SETTINGS optimize_functions_to_subcolumns = 0
)
WHERE explain ILIKE '%Output: m.key_k%';

DROP VIEW issue126641_v;
DROP TABLE issue126641_t;