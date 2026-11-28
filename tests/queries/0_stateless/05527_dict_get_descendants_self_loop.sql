
-- https://github.com/ClickHouse/ClickHouse/issues/69145
-- `dictGetDescendants` returned [] for a repeated self-parented key when its first occurrence was the first key of the block.

DROP DICTIONARY IF EXISTS dict_hashed_05527;
DROP DICTIONARY IF EXISTS dict_flat_05527;
DROP TABLE IF EXISTS tree_05527;

CREATE TABLE tree_05527 (id UInt64, parent_id UInt64) ENGINE = Memory;
INSERT INTO tree_05527 VALUES (1, 1), (2, 1), (1000, 2), (2000, 2);

CREATE DICTIONARY dict_hashed_05527 (id UInt64, parent_id UInt64 HIERARCHICAL)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'tree_05527')) LIFETIME(0) LAYOUT(HASHED());

CREATE DICTIONARY dict_flat_05527 (id UInt64, parent_id UInt64 HIERARCHICAL)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'tree_05527')) LIFETIME(0) LAYOUT(FLAT());

SELECT 'hashed';
SELECT groupArray(dictGetDescendants('dict_hashed_05527', id)) FROM (SELECT arrayJoin([1, 1, 2, 2]) AS id);
SELECT arrayMap(x -> dictGetDescendants('dict_hashed_05527', x), materialize([1, 1]));
SELECT groupArray(dictGetDescendants('dict_hashed_05527', id, 0)) FROM (SELECT arrayJoin([1, 1, 2, 2]) AS id);
SELECT groupArray(dictGetDescendants('dict_hashed_05527', id, 1)) FROM (SELECT arrayJoin([1, 1, 2, 2]) AS id);
SELECT groupArray(dictGetDescendants('dict_hashed_05527', id)) FROM (SELECT arrayJoin([2, 1, 1, 1]) AS id);

SELECT 'flat';
SELECT groupArray(dictGetDescendants('dict_flat_05527', id)) FROM (SELECT arrayJoin([1, 1, 2, 2]) AS id);
SELECT arrayMap(x -> dictGetDescendants('dict_flat_05527', x), materialize([1, 1]));
SELECT groupArray(dictGetDescendants('dict_flat_05527', id, 0)) FROM (SELECT arrayJoin([1, 1, 2, 2]) AS id);
SELECT groupArray(dictGetDescendants('dict_flat_05527', id, 1)) FROM (SELECT arrayJoin([1, 1, 2, 2]) AS id);
SELECT groupArray(dictGetDescendants('dict_flat_05527', id)) FROM (SELECT arrayJoin([2, 1, 1, 1]) AS id);

DROP DICTIONARY dict_hashed_05527;
DROP DICTIONARY dict_flat_05527;
DROP TABLE tree_05527;
