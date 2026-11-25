-- Tags: zookeeper
-- Tag zookeeper: the test creates a Replicated database.

-- A refresh that takes longer than its period leaves the next one due when it completes.
-- `SYSTEM WAIT VIEW` returns when the requested refresh completes, not after the next refresh's delay.

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/clickhouse/{database}/wait_view', 'shard1', 'replica1') FORMAT Null;
USE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 SECOND SETTINGS refresh_coordination_jitter_ms = 3600000
    ENGINE = ReplicatedMergeTree ORDER BY tuple() EMPTY AS SELECT sleep(3) AS x FORMAT Null;
SYSTEM REFRESH VIEW mv;
SYSTEM WAIT VIEW mv;
SELECT count() FROM mv;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
