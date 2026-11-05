-- Tags: no-ordinary-database, no-fasttest, no-replicated-database
-- Tag no-replicated-database: the keys are removed by the replica that drops the table last, possibly on another server

DROP TABLE IF EXISTS keeper_map_drop SYNC;
CREATE TABLE keeper_map_drop (key UInt64, value UInt64)
    ENGINE = KeeperMap('/' || currentDatabase() || '/keeper_map_drop') PRIMARY KEY key;
INSERT INTO keeper_map_drop SELECT number, number FROM numbers(1000);
DROP TABLE keeper_map_drop SYNC;

SYSTEM FLUSH LOGS zookeeper_log;

-- Every key is removed, and no key node is listed.
SELECT countIf(op_num = 'Remove'), countIf(toString(op_num) LIKE '%List%')
FROM system.zookeeper_log
WHERE type = 'Request'
    AND path LIKE '/test_keeper_map/' || currentDatabase() || '/keeper_map_drop/data/%';

SELECT count() FROM system.zookeeper WHERE path = '/test_keeper_map/' || currentDatabase();
