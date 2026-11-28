-- Tags: no-replicated-database
-- Tag no-replicated-database: a `Replicated` database does not support `CREATE ... CLONE AS`.

-- A `TTL` that `CLONE AS` gives the clone itself is written in the session, so its `DateTime` string literal gets the time zone of
-- the session in the stored definition, as in `05459_ttl_datetime_literal_session_timezone`.

SET session_timezone = 'Asia/Tokyo';

DROP TABLE IF EXISTS t_plain;
DROP TABLE IF EXISTS t_clone;

CREATE TABLE t_plain (event DateTime, id UInt8) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_clone CLONE AS t_plain TTL event + INTERVAL 1 SECOND DELETE WHERE event = '2020-01-01 00:00:00';
SELECT extract(engine_full, 'WHERE [^)]*\\)') FROM system.tables WHERE database = currentDatabase() AND name = 't_clone';

DROP TABLE t_plain;
DROP TABLE t_clone;
