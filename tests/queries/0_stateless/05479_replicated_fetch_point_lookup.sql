-- Tags: zookeeper, no-shared-merge-tree, no-replicated-database, no-async-insert
-- no-shared-merge-tree: checks per-replica parts lookups in Keeper.
-- no-replicated-database: uses explicit replica names on one server.
-- no-async-insert: each INSERT must create its own parts.

SET insert_keeper_fault_injection_probability = 0;

DROP TABLE IF EXISTS point_fetch_source SYNC;
DROP TABLE IF EXISTS point_fetch_replica SYNC;
DROP TABLE IF EXISTS point_fetch_marker;

CREATE TABLE point_fetch_source (p UInt32, n UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/point_fetch', 'source')
PARTITION BY p ORDER BY n
SETTINGS old_parts_lifetime = 3600;

CREATE TABLE point_fetch_replica AS point_fetch_source
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/point_fetch', 'replica')
PARTITION BY p ORDER BY n
SETTINGS old_parts_lifetime = 3600;

CREATE TABLE point_fetch_marker (t DateTime64(6)) ENGINE = Memory;
SYSTEM SYNC REPLICA point_fetch_replica LIGHTWEIGHT;
SYSTEM STOP MERGES point_fetch_source;
SYSTEM STOP MERGES point_fetch_replica;
SYSTEM STOP FETCHES point_fetch_replica;

-- Leave the insert entries queued until the source has many parts.
INSERT INTO point_fetch_source SELECT number, number FROM numbers(32);
SYSTEM SYNC REPLICA point_fetch_replica PULL;

-- Check that `List` requests are observable for this path. The marker below
-- excludes this controlled listing from the fetch measurement.
SELECT 'source parts probe', count() > 0
FROM system.zookeeper
WHERE path = '/clickhouse/tables/' || currentDatabase() || '/point_fetch/replicas/source/parts';
SYSTEM FLUSH LOGS zookeeper_log;
SELECT 'parts listing requests observed', count() > 0
FROM system.zookeeper_log
WHERE event_date >= yesterday()
    AND type = 'Request'
    AND op_num IN ('List', 'SimpleList', 'FilteredList')
    AND path = '/clickhouse/tables/' || currentDatabase() || '/point_fetch/replicas/source/parts';

INSERT INTO point_fetch_marker SELECT now64(6);
SYSTEM START FETCHES point_fetch_replica;
SYSTEM SYNC REPLICA point_fetch_replica LIGHTWEIGHT;
SYSTEM FLUSH LOGS zookeeper_log;

SELECT 'exact parts', count(), sum(n) FROM point_fetch_replica;
-- Require point requests in the same window so an empty Request log cannot pass.
SELECT 'exact lookup avoids listing', countIf(op_num = 'Exists') > 0
    AND countIf(op_num IN ('List', 'SimpleList', 'FilteredList')) = 0
FROM system.zookeeper_log
WHERE event_date >= yesterday()
    AND event_time >= (SELECT max(t) FROM point_fetch_marker)
    AND type = 'Request'
    AND (path = '/clickhouse/tables/' || currentDatabase() || '/point_fetch/replicas/source/parts'
        OR startsWith(path, '/clickhouse/tables/' || currentDatabase() || '/point_fetch/replicas/source/parts/'));

SYSTEM STOP FETCHES point_fetch_replica;
-- Keep one `GET_PART` in this range. Concurrent covering fetches can be postponed
-- by `future_parts` and trigger independent listing by the part checker.
INSERT INTO point_fetch_source VALUES (0, 100), (0, 101), (0, 102);
SYSTEM START MERGES point_fetch_source;
OPTIMIZE TABLE point_fetch_source PARTITION 0 FINAL;
SYSTEM STOP MERGES point_fetch_source;
SYSTEM SYNC REPLICA point_fetch_replica PULL;
TRUNCATE TABLE point_fetch_marker;
INSERT INTO point_fetch_marker SELECT now64(6);
SYSTEM START FETCHES point_fetch_replica;
-- `LIGHTWEIGHT` waits for fetches without waiting for the stopped `MERGE_PARTS` entries.
SYSTEM SYNC REPLICA point_fetch_replica LIGHTWEIGHT;
SYSTEM FLUSH LOGS zookeeper_log;

-- The originals remain as `Outdated` parts on the source. Fetch the covering
-- part already known to the queue instead of fetching its predecessors one by one.
SELECT 'covering part', count(), sum(n) FROM point_fetch_replica WHERE p = 0;
SELECT 'one merged part', count(), min(level) > 0
FROM system.parts
WHERE database = currentDatabase() AND table = 'point_fetch_replica'
    AND partition_id = '0' AND active;
SELECT 'covering lookup avoids listing', countIf(op_num = 'Exists') > 0
    AND countIf(op_num IN ('List', 'SimpleList', 'FilteredList')) = 0
FROM system.zookeeper_log
WHERE event_date >= yesterday()
    AND event_time >= (SELECT max(t) FROM point_fetch_marker)
    AND type = 'Request'
    AND (path = '/clickhouse/tables/' || currentDatabase() || '/point_fetch/replicas/source/parts'
        OR startsWith(path, '/clickhouse/tables/' || currentDatabase() || '/point_fetch/replicas/source/parts/'));

SYSTEM START MERGES point_fetch_replica;
DROP TABLE point_fetch_replica SYNC;
DROP TABLE point_fetch_source SYNC;
DROP TABLE point_fetch_marker;
