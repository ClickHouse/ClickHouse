-- A table whose `TTL ... WHERE` has an `IN` subquery must still attach after a table read behind that
-- subquery (through a view or a `Distributed` table) is dropped: the subquery is needed only when the TTL
-- is applied. A subquery of a TTL is resolved outside of the current database, so the tables are qualified.

DROP TABLE IF EXISTS ttl_in_view;
DROP TABLE IF EXISTS ttl_in_dist;
DROP TABLE IF EXISTS ttl_exists_view;
DROP TABLE IF EXISTS ttl_src_view;
DROP TABLE IF EXISTS ttl_src_dist;
DROP TABLE IF EXISTS ttl_src;

CREATE TABLE ttl_src (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE VIEW ttl_src_view AS SELECT x FROM ttl_src;
CREATE TABLE ttl_src_dist (x UInt64) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), ttl_src);

CREATE TABLE ttl_in_view (x UInt64, d DateTime) ENGINE = MergeTree ORDER BY x
TTL d + INTERVAL 1 SECOND WHERE x IN (SELECT x FROM {CLICKHOUSE_DATABASE:Identifier}.ttl_src_view);
CREATE TABLE ttl_in_dist (x UInt64, d DateTime) ENGINE = MergeTree ORDER BY x
TTL d + INTERVAL 1 SECOND WHERE x IN {CLICKHOUSE_DATABASE:Identifier}.ttl_src_dist;
CREATE TABLE ttl_exists_view (x UInt64, d DateTime) ENGINE = MergeTree ORDER BY x
TTL d + INTERVAL 1 SECOND WHERE x > 1 AND exists(SELECT 1 FROM {CLICKHOUSE_DATABASE:Identifier}.ttl_src_view);

-- The source is empty, so no TTL merge can delete a row before it is dropped.
INSERT INTO ttl_in_view VALUES (1, now() - 100), (2, now() - 100);
INSERT INTO ttl_in_dist VALUES (1, now() - 100), (2, now() - 100);
INSERT INTO ttl_exists_view VALUES (1, now() - 100), (2, now() - 100);

DROP TABLE ttl_src;

DETACH TABLE ttl_in_view;
ATTACH TABLE ttl_in_view;
DETACH TABLE ttl_in_dist;
ATTACH TABLE ttl_in_dist;
DETACH TABLE ttl_exists_view;
ATTACH TABLE ttl_exists_view;

SELECT x FROM ttl_in_view ORDER BY x;

-- Once the source is back, the TTL plans its subquery when it is applied. No row matched the `WHERE` when
-- it was inserted, so a merge would not apply the TTL; `MATERIALIZE TTL` recalculates it.
CREATE TABLE ttl_src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO ttl_src VALUES (1);

ALTER TABLE ttl_in_view MATERIALIZE TTL SETTINGS mutations_sync = 2;
ALTER TABLE ttl_in_dist MATERIALIZE TTL SETTINGS mutations_sync = 2;
ALTER TABLE ttl_exists_view MATERIALIZE TTL SETTINGS mutations_sync = 2;

SELECT x FROM ttl_in_view ORDER BY x;
SELECT x FROM ttl_in_dist ORDER BY x;
SELECT x FROM ttl_exists_view ORDER BY x;

DROP TABLE ttl_in_view;
DROP TABLE ttl_in_dist;
DROP TABLE ttl_exists_view;
DROP TABLE ttl_src_view;
DROP TABLE ttl_src_dist;
DROP TABLE ttl_src;
