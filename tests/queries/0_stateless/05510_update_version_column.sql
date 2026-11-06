-- UPDATE of the version column of ReplacingMergeTree.

SET mutations_sync = 2;

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
