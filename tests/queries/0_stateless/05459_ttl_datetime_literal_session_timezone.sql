-- The `TTL` of a table is evaluated by background merges without the session, so a `DateTime` string literal in it was
-- read in the server time zone instead of `session_timezone`, and `TTL ... DELETE WHERE event = '...'` removed other rows
-- than it named. The literal now gets the time zone of the session in the stored definition, as in the mutations. The
-- merges run without the session time zone here, like background merges.

SET session_timezone = 'Asia/Tokyo';

DROP TABLE IF EXISTS t_create;
DROP TABLE IF EXISTS t_modify;

CREATE TABLE t_create (event DateTime, id UInt8) ENGINE = MergeTree ORDER BY id
TTL event + INTERVAL 1 SECOND DELETE WHERE event = '2020-01-01 00:00:00'
SETTINGS merge_with_ttl_timeout = 86400;
INSERT INTO t_create VALUES ('2020-01-01 00:00:00', 1), ('2020-01-01 09:00:00', 2);
OPTIMIZE TABLE t_create FINAL SETTINGS session_timezone = '';
SELECT 'CREATE', groupArray(id) FROM (SELECT id FROM t_create ORDER BY id);
SELECT extract(engine_full, 'WHERE [^)]*\\)') FROM system.tables WHERE database = currentDatabase() AND name = 't_create';

CREATE TABLE t_modify (event DateTime, id UInt8) ENGINE = MergeTree ORDER BY id SETTINGS merge_with_ttl_timeout = 86400;
INSERT INTO t_modify VALUES ('2020-01-01 00:00:00', 1), ('2020-01-01 09:00:00', 2);
ALTER TABLE t_modify MODIFY TTL event + INTERVAL 1 SECOND DELETE WHERE event = '2020-01-01 00:00:00';
OPTIMIZE TABLE t_modify FINAL SETTINGS session_timezone = '';
SELECT 'MODIFY TTL', groupArray(id) FROM (SELECT id FROM t_modify ORDER BY id);

DROP TABLE t_create;
DROP TABLE t_modify;
