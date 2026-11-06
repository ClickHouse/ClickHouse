-- UPDATE of the sign and version columns of the Collapsing and Replacing engines.

SET mutations_sync = 2;

SELECT '-- Sign column';
CREATE TABLE c (key UInt64, value String, sign Int8) ENGINE = CollapsingMergeTree(sign) ORDER BY key
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO c SETTINGS optimize_on_insert = 0 VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', 1);
-- The cancel rows were inserted with the wrong sign.
INSERT INTO c SETTINGS optimize_on_insert = 0 VALUES (1, 'cancel', 1), (2, 'cancel', 1);
SELECT * FROM c FINAL ORDER BY key;

ALTER TABLE c UPDATE sign = -1 WHERE key = 1 AND value = 'cancel';
SELECT * FROM c FINAL ORDER BY key;

UPDATE c SET sign = -1 WHERE key = 2 AND value = 'cancel';
SELECT * FROM c FINAL ORDER BY key;

OPTIMIZE TABLE c FINAL;
SELECT * FROM c ORDER BY key;

SELECT '-- Version column';
CREATE TABLE r (key UInt64, value String, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY key
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO r SETTINGS optimize_on_insert = 0 VALUES (1, 'a', 1), (1, 'b', 2), (2, 'c', 1), (2, 'd', 2);
SELECT * FROM r FINAL ORDER BY key;

ALTER TABLE r UPDATE ver = 3 WHERE value = 'a';
SELECT * FROM r FINAL ORDER BY key;

UPDATE r SET ver = 3 WHERE value = 'c';
SELECT * FROM r FINAL ORDER BY key;

OPTIMIZE TABLE r FINAL;
SELECT * FROM r ORDER BY key;

SELECT '-- Version subcolumn';
CREATE TABLE s (key UInt64, value String, meta Tuple(ver UInt64, del UInt8)) ENGINE = ReplacingMergeTree(meta.ver, meta.del) ORDER BY key;

INSERT INTO s SETTINGS optimize_on_insert = 0 VALUES (1, 'a', (1, 0)), (1, 'b', (2, 0)), (2, 'c', (1, 0));
SELECT * FROM s FINAL ORDER BY key;

ALTER TABLE s UPDATE meta = (3, 0) WHERE value = 'a';
SELECT * FROM s FINAL ORDER BY key;

SELECT '-- Version in a MATERIALIZED column';
CREATE TABLE m (key UInt64, v UInt64, meta Tuple(ver UInt64) MATERIALIZED tuple(v)) ENGINE = ReplacingMergeTree(meta.ver) ORDER BY key;

INSERT INTO m (key, v) SETTINGS optimize_on_insert = 0 VALUES (1, 2), (1, 1);
SELECT v FROM m FINAL;

ALTER TABLE m UPDATE v = 3 WHERE v = 1;
SELECT v FROM m FINAL;
