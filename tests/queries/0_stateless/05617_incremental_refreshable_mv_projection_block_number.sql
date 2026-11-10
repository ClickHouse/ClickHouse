-- Tags: atomic-database
-- A normal projection does not store `_block_number` and `_block_offset`. Reading them through it must not
-- return the projection part's own values: the incremental refresh filters the source by these columns, so
-- wrong values make it skip rows and advance its cursor past them.

DROP TABLE IF EXISTS proj_mv;
DROP TABLE IF EXISTS proj_tgt;
DROP TABLE IF EXISTS proj_src;

CREATE TABLE proj_src (id UInt64, ts DateTime, v String, PROJECTION by_id (SELECT * ORDER BY id))
ENGINE = MergeTree ORDER BY ts
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    add_minmax_index_for_block_number_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    part_minmax_index_columns = 'with_block_number_offset',
    index_granularity = 4;

CREATE TABLE proj_tgt (id UInt64, ts DateTime, v String) ENGINE = MergeTree ORDER BY id;

-- The `WHERE` on `id` makes `by_id` the cheaper read for the source.
CREATE MATERIALIZED VIEW proj_mv
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL
    TO proj_tgt EMPTY
    AS SELECT id, ts, v FROM proj_src WHERE id > 12 AND id < 28;

-- Round 1: ids 0..9, none of them match the `WHERE`.
INSERT INTO proj_src SELECT number, toDateTime('2026-01-01 00:00:00') - number, toString(number) FROM numbers(10);
SYSTEM REFRESH VIEW proj_mv;
SYSTEM WAIT VIEW proj_mv;
SELECT 'round1', count() FROM proj_tgt;

-- Round 2: ids 10..29 are merged together with the already exported part before the refresh,
-- so all rows of the merged part share the oldest block number in the projection.
INSERT INTO proj_src SELECT number + 10, toDateTime('2026-01-01 00:00:00') - number, toString(number) FROM numbers(10);
INSERT INTO proj_src SELECT number + 20, toDateTime('2026-01-01 00:00:00') - number, toString(number) FROM numbers(10);
OPTIMIZE TABLE proj_src FINAL;
SYSTEM REFRESH VIEW proj_mv;
SYSTEM WAIT VIEW proj_mv;
SELECT 'round2', count(), min(id), max(id) FROM proj_tgt;

-- Round 3: nothing new; the rows skipped earlier must not stay missing.
SYSTEM REFRESH VIEW proj_mv;
SYSTEM WAIT VIEW proj_mv;
SELECT 'round3', count() FROM proj_tgt;

-- The same rows read directly: results with and without the projection must agree.
SELECT 'no projection', id, _block_number, _block_offset FROM proj_src WHERE id IN (5, 15) ORDER BY id SETTINGS optimize_use_projections = 0;
SELECT 'projection', id, _block_number, _block_offset FROM proj_src WHERE id IN (5, 15) ORDER BY id SETTINGS optimize_use_projections = 1;
SELECT 'filter no projection', count() FROM proj_src WHERE id = 15 AND _block_number = 2 SETTINGS optimize_use_projections = 0;
SELECT 'filter projection', count() FROM proj_src WHERE id = 15 AND _block_number = 2 SETTINGS optimize_use_projections = 1;

DROP TABLE proj_mv;
DROP TABLE proj_tgt;
DROP TABLE proj_src;
