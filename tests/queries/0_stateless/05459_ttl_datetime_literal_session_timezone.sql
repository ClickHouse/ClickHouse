-- The `TTL` of a table is evaluated by background merges without the session, so a `DateTime` string literal in it was
-- read in the server time zone instead of `session_timezone`, and `TTL ... DELETE WHERE event = '...'` removed other rows
-- than it named. The literal now gets the time zone of the session in the stored definition, as in the mutations. The
-- merges run without the session time zone here, like background merges.

SET session_timezone = 'Asia/Tokyo';

DROP TABLE IF EXISTS t_create;
DROP TABLE IF EXISTS t_modify;
DROP TABLE IF EXISTS t_source;
DROP TABLE IF EXISTS t_copy;
DROP TABLE IF EXISTS t_plain;
DROP TABLE IF EXISTS t_clone;
DROP TABLE IF EXISTS t_modify_column;
DROP TABLE IF EXISTS t_add_column;

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

-- A `TTL` that `CREATE TABLE ... AS` copies from the source table keeps the meaning it has there: here, a literal written
-- without the session time zone.
SET session_timezone = '';
CREATE TABLE t_source (event DateTime, id UInt8) ENGINE = MergeTree ORDER BY id
TTL event + INTERVAL 1 SECOND DELETE WHERE event = '2020-01-01 00:00:00';
SET session_timezone = 'Asia/Tokyo';
CREATE TABLE t_copy AS t_source;
SELECT 'AS', extract(engine_full, 'WHERE [^\']*\'[^\']*\'') FROM system.tables WHERE database = currentDatabase() AND name = 't_copy';

-- A `TTL` that `CLONE AS` gives the clone itself is written in the session.
CREATE TABLE t_plain (event DateTime, id UInt8) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_clone CLONE AS t_plain TTL event + INTERVAL 1 SECOND DELETE WHERE event = '2020-01-01 00:00:00';
SELECT 'CLONE AS', extract(engine_full, 'WHERE [^)]*\\)') FROM system.tables WHERE database = currentDatabase() AND name = 't_clone';

-- The type of the column is the one that the preceding commands of the `ALTER` give it: here, a literal of a `UTC` column.
CREATE TABLE t_modify_column (event DateTime, id UInt8) ENGINE = MergeTree ORDER BY id SETTINGS merge_with_ttl_timeout = 86400;
INSERT INTO t_modify_column VALUES (1577836800, 1), (1577804400, 2);
ALTER TABLE t_modify_column MODIFY COLUMN event DateTime('UTC'),
    MODIFY TTL event + INTERVAL 1 SECOND DELETE WHERE event = '2020-01-01 00:00:00';
OPTIMIZE TABLE t_modify_column FINAL SETTINGS session_timezone = '';
SELECT 'MODIFY COLUMN, MODIFY TTL', groupArray(id) FROM (SELECT id FROM t_modify_column ORDER BY id);

CREATE TABLE t_add_column (id UInt8) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_add_column ADD COLUMN event DateTime, MODIFY TTL event + INTERVAL 1 SECOND DELETE WHERE event = '2020-01-01 00:00:00';
SELECT 'ADD COLUMN, MODIFY TTL', extract(engine_full, 'WHERE [^)]*\\)') FROM system.tables WHERE database = currentDatabase() AND name = 't_add_column';

DROP TABLE t_create;
DROP TABLE t_modify;
DROP TABLE t_source;
DROP TABLE t_copy;
DROP TABLE t_plain;
DROP TABLE t_clone;
DROP TABLE t_modify_column;
DROP TABLE t_add_column;
