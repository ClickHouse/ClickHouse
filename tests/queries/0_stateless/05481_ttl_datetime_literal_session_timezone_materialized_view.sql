-- The `TTL` of the inner table of a materialized view gets the time zone of the session too, as in
-- `05459_ttl_datetime_literal_session_timezone`. In a `Replicated` database, as with `ON CLUSTER`, the query runs again on a DDL
-- worker, which does not rewrite it, so the initiator rewrites the `TTL` of the inner engine before sending the query.

SET session_timezone = 'Asia/Tokyo';
SET distributed_ddl_output_mode = 'none';

DROP TABLE IF EXISTS t_src;
DROP TABLE IF EXISTS t_mv;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE t_src (event DateTime, id UInt8) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW t_mv ENGINE = MergeTree ORDER BY id
TTL event + INTERVAL 1 SECOND DELETE WHERE event = '2020-01-01 00:00:00' AS SELECT event, id FROM t_src;
SELECT 'local', extract(engine_full, 'WHERE [^)]*\\)') FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner%';

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/test/05481/' || currentDatabase(), 's1', 'r1');
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_src (event DateTime, id UInt8) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.t_mv ENGINE = MergeTree ORDER BY id
TTL event + INTERVAL 1 SECOND DELETE WHERE event = '2020-01-01 00:00:00' AS SELECT event, id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_src;
SELECT 'Replicated', extract(engine_full, 'WHERE [^)]*\\)') FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name LIKE '.inner%';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE t_mv;
DROP TABLE t_src;
