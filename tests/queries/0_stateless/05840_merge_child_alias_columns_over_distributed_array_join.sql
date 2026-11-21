-- A `Distributed` child of a `Merge` read at `FetchColumns` (here because of the `ARRAY JOIN`) names its
-- columns by column identifiers, and the `ALIAS` columns of the child are evaluated over them by the `Merge`.
-- An `ALIAS` over the child's own `_table` sees the value of the child, not the `Merge` virtual column.

DROP TABLE IF EXISTS merge_alias_aj;
DROP TABLE IF EXISTS merge_alias_aj_dist;
DROP TABLE IF EXISTS merge_alias_aj_child;

CREATE TABLE merge_alias_aj_child
(
    p UInt8,
    q String,
    y String ALIAS _table,
    z String ALIAS concat(q, '-', toString(p + 1)),
    d UInt8 ALIAS _database = currentDatabase()
)
ENGINE = MergeTree ORDER BY p;
INSERT INTO merge_alias_aj_child (p, q) VALUES (1, 'a'), (2, 'b');
CREATE TABLE merge_alias_aj_dist AS merge_alias_aj_child ENGINE = Distributed(test_shard_localhost, currentDatabase(), merge_alias_aj_child);
CREATE TABLE merge_alias_aj (p UInt8, q String, y String, z String, d UInt8) ENGINE = Merge(currentDatabase(), '^merge_alias_aj_dist$');

SET prefer_localhost_replica = 0;

SELECT _table, y FROM merge_alias_aj ARRAY JOIN [1] AS one;
SELECT z FROM merge_alias_aj ARRAY JOIN [1] AS one ORDER BY z;
SELECT q, z, p FROM merge_alias_aj ARRAY JOIN [1] AS one ORDER BY p;
SELECT _table, _database = currentDatabase(), y, d, z, q, p FROM merge_alias_aj ARRAY JOIN [1] AS one ORDER BY p;
SELECT y, count() FROM merge_alias_aj ARRAY JOIN [1, 2] AS one WHERE _table = 'merge_alias_aj_dist' GROUP BY y;
SELECT _table, y FROM merge_alias_aj;

SET prefer_localhost_replica = 1;

SELECT _table, y FROM merge_alias_aj ARRAY JOIN [1] AS one;
SELECT _table, y, d, z FROM merge_alias_aj ARRAY JOIN [1] AS one ORDER BY z;

DROP TABLE merge_alias_aj;
DROP TABLE merge_alias_aj_dist;
DROP TABLE merge_alias_aj_child;
