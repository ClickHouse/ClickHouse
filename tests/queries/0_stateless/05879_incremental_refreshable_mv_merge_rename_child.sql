-- Tags: atomic-database
-- The cursor of an incremental refresh from a `Merge` table is persisted per table UUID, regardless of the table name:
-- a table that is renamed to another name still matched by the `Merge` table keeps its position, and its rows are not
-- appended again. A table dropped and created again under the same name is read from the beginning.

DROP TABLE IF EXISTS mrc_mv;
DROP TABLE IF EXISTS mrc_dst;
DROP TABLE IF EXISTS mrc_m;
DROP TABLE IF EXISTS mrc_t1;
DROP TABLE IF EXISTS mrc_t2;

CREATE TABLE mrc_t1 (a UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
CREATE TABLE mrc_m (a UInt64) ENGINE = Merge(currentDatabase(), '^mrc_t[12]$');
CREATE TABLE mrc_dst (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE MATERIALIZED VIEW mrc_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO mrc_dst EMPTY AS SELECT a FROM mrc_m;

INSERT INTO mrc_t1 VALUES (1), (2);
SYSTEM REFRESH VIEW mrc_mv;
SYSTEM WAIT VIEW mrc_mv;
SELECT 'first', arraySort(groupArray(a)) FROM mrc_dst;

-- The renamed table still matches the `Merge` table: nothing is appended.
RENAME TABLE mrc_t1 TO mrc_t2;
SYSTEM REFRESH VIEW mrc_mv;
SYSTEM WAIT VIEW mrc_mv;
SELECT 'renamed', arraySort(groupArray(a)) FROM mrc_dst;

-- New rows in the renamed table: only they are appended.
INSERT INTO mrc_t2 VALUES (3);
SYSTEM REFRESH VIEW mrc_mv;
SYSTEM WAIT VIEW mrc_mv;
SELECT 'inserted', arraySort(groupArray(a)) FROM mrc_dst;

-- A table created again under the same name is read from the beginning.
DROP TABLE mrc_t2 SYNC;
CREATE TABLE mrc_t2 (a UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO mrc_t2 VALUES (4);
SYSTEM REFRESH VIEW mrc_mv;
SYSTEM WAIT VIEW mrc_mv;
SELECT 'recreated', arraySort(groupArray(a)) FROM mrc_dst;

DROP TABLE mrc_mv;
DROP TABLE mrc_dst;
DROP TABLE mrc_m;
DROP TABLE mrc_t2;
