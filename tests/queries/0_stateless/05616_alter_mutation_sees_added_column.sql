-- Tags: no-replicated-database
-- no-replicated-database: a Replicated database rejects an ALTER that mixes commands of different types.
-- Random settings limits: enable_shared_storage_snapshot_in_query=(1, None)

SET mutations_sync = 2;

DROP TABLE IF EXISTS t_alias;
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS t_replicated;

CREATE TABLE t (id UInt8, v UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO t VALUES (1, 0);
ALTER TABLE t ADD COLUMN c UInt8, UPDATE c = 1 WHERE c = 0;
ALTER TABLE t UPDATE v = 1 WHERE 1, ADD COLUMN d UInt8, UPDATE d = 2 WHERE d = 0;
CREATE TABLE t_alias ENGINE = Alias('t');
ALTER TABLE t_alias ADD COLUMN e UInt8, UPDATE e = 3 WHERE e = 0;
SELECT * FROM t ORDER BY id;

CREATE TABLE src (id UInt8, v UInt8, c UInt8, d UInt8, e UInt8, f UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO src VALUES (2, 2, 2, 2, 2, 2);
ALTER TABLE t ADD COLUMN f UInt8, ATTACH PARTITION tuple() FROM src;
SELECT * FROM t ORDER BY id;

CREATE TABLE t2 (id UInt8, v UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO t2 VALUES (1, 1), (2, 2);
ALTER TABLE t2 RENAME COLUMN v TO w, UPDATE w = 3 WHERE w = 1;
ALTER TABLE t2 ADD COLUMN c UInt8, DELETE WHERE c = 0 AND id = 2;
CREATE TABLE dst (id UInt8, w UInt8, c UInt8, e UInt8) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t2 ADD COLUMN e UInt8, MOVE PARTITION tuple() TO TABLE dst;
SELECT * FROM dst ORDER BY id;

CREATE TABLE t_replicated (id UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_replicated', 'r1') ORDER BY id;
INSERT INTO t_replicated VALUES (1);
ALTER TABLE t_replicated ADD COLUMN c UInt8, UPDATE c = 1 WHERE c = 0;
SELECT * FROM t_replicated;

DROP TABLE t_alias;
DROP TABLE t;
DROP TABLE src;
DROP TABLE t2;
DROP TABLE dst;
DROP TABLE t_replicated;
