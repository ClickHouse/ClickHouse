-- Tags: no-ordinary-database, no-fasttest, no-replicated-database
-- Tag no-replicated-database: the keys are removed by the replica that drops the table last, possibly on another server

DROP TABLE IF EXISTS keeper_map_drop_small SYNC;
DROP TABLE IF EXISTS keeper_map_drop SYNC;
CREATE TABLE keeper_map_drop_small (key UInt64, value UInt64)
    ENGINE = KeeperMap('/' || currentDatabase() || '/keeper_map_drop_small') PRIMARY KEY key;
CREATE TABLE keeper_map_drop (key UInt64, value UInt64)
    ENGINE = KeeperMap('/' || currentDatabase() || '/keeper_map_drop') PRIMARY KEY key;
-- With data/ itself, 1000 nodes: the remove_nodes_limit of RemoveRecursive. One more key makes it fall back to batches.
INSERT INTO keeper_map_drop_small SELECT number, number FROM numbers(999);
INSERT INTO keeper_map_drop SELECT number, number FROM numbers(1000);
DROP TABLE keeper_map_drop_small SYNC;
DROP TABLE keeper_map_drop SYNC;

SYSTEM FLUSH LOGS zookeeper_log;

-- RemoveRecursive of data/ in the same multi request as the removal of metadata/, removals of data/ in any other request, then removals and List requests of the key nodes.
WITH '/test_keeper_map/' || currentDatabase() || '/' AS prefix
SELECT
    splitByChar('/', substring(path, length(prefix) + 1))[1] AS table_name,
    countIf(op_num = 'RemoveRecursive' AND endsWith(path, '/data') AND removes_metadata),
    countIf(toString(op_num) IN ('Remove', 'TryRemove', 'RemoveRecursive') AND endsWith(path, '/data') AND NOT removes_metadata),
    countIf(toString(op_num) IN ('Remove', 'TryRemove', 'RemoveRecursive') AND path LIKE '%/data/%'),
    countIf(toString(op_num) LIKE '%List%' AND path LIKE '%/data/%')
FROM
(
    SELECT path, op_num, max(op_num = 'Remove' AND endsWith(path, '/metadata')) OVER (PARTITION BY session_id, xid) AS removes_metadata
    FROM system.zookeeper_log
    WHERE type = 'Request' AND startsWith(path, prefix)
)
GROUP BY table_name
ORDER BY table_name;

SELECT count() FROM system.zookeeper WHERE path = '/test_keeper_map/' || currentDatabase();
