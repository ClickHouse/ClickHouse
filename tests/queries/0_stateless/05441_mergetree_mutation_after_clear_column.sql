-- A mutation queued after a pending CLEAR/DROP COLUMN (or DROP INDEX/PROJECTION) of a part must not be lost.

DROP TABLE IF EXISTS t;

SELECT 'clear b, update b';
CREATE TABLE t (id UInt32, a UInt32, b UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t VALUES (1, 1111, 2222), (2, 3333, 4444);
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active;
SYSTEM STOP MERGES t;
ALTER TABLE t CLEAR COLUMN b SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE t UPDATE b = 5 WHERE id = 1 SETTINGS alter_sync = 0, mutations_sync = 0;
SYSTEM START MERGES t;
ALTER TABLE t UPDATE a = a WHERE 1 SETTINGS mutations_sync = 2;
SELECT id, a, b FROM t ORDER BY id;
DROP TABLE t;

SELECT 'clear b, update b reading b';
CREATE TABLE t (id UInt32, a UInt32, b UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t VALUES (1, 1111, 2222), (2, 3333, 4444);
SYSTEM STOP MERGES t;
ALTER TABLE t CLEAR COLUMN b SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE t UPDATE b = b + 7 WHERE 1 SETTINGS alter_sync = 0, mutations_sync = 0;
SYSTEM START MERGES t;
ALTER TABLE t UPDATE a = a WHERE 1 SETTINGS mutations_sync = 2;
SELECT id, a, b FROM t ORDER BY id;
DROP TABLE t;

SELECT 'update a, clear b, update b';
CREATE TABLE t (id UInt32, a UInt32, b UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t VALUES (1, 1111, 2222), (2, 3333, 4444);
SYSTEM STOP MERGES t;
ALTER TABLE t UPDATE a = a + 1 WHERE 1 SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE t CLEAR COLUMN b SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE t UPDATE b = a WHERE 1 SETTINGS alter_sync = 0, mutations_sync = 0;
SYSTEM START MERGES t;
ALTER TABLE t UPDATE a = a WHERE 1 SETTINGS mutations_sync = 2;
SELECT id, a, b FROM t ORDER BY id;
DROP TABLE t;

SELECT 'update a, clear b';
CREATE TABLE t (id UInt32, a UInt32, b UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t VALUES (1, 1111, 2222), (2, 3333, 4444);
SYSTEM STOP MERGES t;
ALTER TABLE t UPDATE a = a + 1 WHERE 1 SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE t CLEAR COLUMN b SETTINGS alter_sync = 0, mutations_sync = 0;
SYSTEM START MERGES t;
ALTER TABLE t UPDATE a = a WHERE 1 SETTINGS mutations_sync = 2;
SELECT id, a, b FROM t ORDER BY id;
DROP TABLE t;

SELECT 'drop b, add b, update b';
CREATE TABLE t (id UInt32, a UInt32, b UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t VALUES (1, 1111, 2222), (2, 3333, 4444);
SYSTEM STOP MERGES t;
ALTER TABLE t DROP COLUMN b SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE t ADD COLUMN b UInt32 SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE t UPDATE b = 5 WHERE id = 1 SETTINGS alter_sync = 0, mutations_sync = 0;
SYSTEM START MERGES t;
ALTER TABLE t UPDATE a = a WHERE 1 SETTINGS mutations_sync = 2;
SELECT id, a, b FROM t ORDER BY id;
DROP TABLE t;

SELECT 'drop index, add index, materialize index';
CREATE TABLE t (id UInt32, a UInt32, b UInt32, INDEX idx b TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t VALUES (1, 1111, 2222), (2, 3333, 4444);
SYSTEM STOP MERGES t;
ALTER TABLE t DROP INDEX idx SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE t ADD INDEX idx b TYPE minmax GRANULARITY 1 SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE t MATERIALIZE INDEX idx SETTINGS alter_sync = 0, mutations_sync = 0;
SYSTEM START MERGES t;
ALTER TABLE t UPDATE a = a WHERE 1 SETTINGS mutations_sync = 2;
SELECT secondary_indices_uncompressed_bytes > 0 FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active;
DROP TABLE t;

SELECT 'drop projection, add projection, materialize projection';
CREATE TABLE t (id UInt32, a UInt32, b UInt32, PROJECTION p (SELECT b, count() GROUP BY b)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t VALUES (1, 1111, 2222), (2, 3333, 4444);
SYSTEM STOP MERGES t;
ALTER TABLE t DROP PROJECTION p SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE t ADD PROJECTION p (SELECT b, count() GROUP BY b) SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE t MATERIALIZE PROJECTION p SETTINGS alter_sync = 0, mutations_sync = 0;
SYSTEM START MERGES t;
ALTER TABLE t UPDATE a = a WHERE 1 SETTINGS mutations_sync = 2;
SELECT projections FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't' AND NOT is_done;
DROP TABLE t;
