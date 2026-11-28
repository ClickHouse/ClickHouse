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

-- A projection that is not read can still prune parent parts through its indexes; those must not see synthesized values either.
DROP TABLE IF EXISTS proj_filter;

CREATE TABLE proj_filter (id UInt64, v String, PROJECTION p (SELECT * WHERE id = 15 ORDER BY id))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    part_minmax_index_columns = 'with_block_number_offset',
    index_granularity = 4;

INSERT INTO proj_filter SELECT number, toString(number) FROM numbers(20);

SELECT 'offset filter no projection', count() FROM proj_filter WHERE id = 15 AND _block_offset = 15 SETTINGS optimize_use_projections = 0;
SELECT 'offset filter projection', count() FROM proj_filter WHERE id = 15 AND _block_offset = 15 SETTINGS optimize_use_projections = 1;
SELECT 'offset filter projection filtering', count() FROM proj_filter WHERE id = 15 AND _block_offset = 15 SETTINGS optimize_use_projections = 1, optimize_use_projection_filtering = 1;

DROP TABLE proj_filter;

-- A projection that stores the columns explicitly stays usable and returns the stored values.
DROP TABLE IF EXISTS proj_stored;

CREATE TABLE proj_stored (id UInt64, v String, PROJECTION p (SELECT *, _block_number, _block_offset ORDER BY id))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    allow_commit_order_projection = 1,
    index_granularity = 4;

INSERT INTO proj_stored SELECT number, toString(number) FROM numbers(10);
INSERT INTO proj_stored SELECT number + 10, toString(number) FROM numbers(10);
OPTIMIZE TABLE proj_stored FINAL;

SELECT 'stored projection', id, _block_number, _block_offset FROM proj_stored WHERE id IN (5, 15) ORDER BY id SETTINGS force_optimize_projection_name = 'p';

DROP TABLE proj_stored;

-- An implicit minmax index on `_block_number` is built only for a projection that stores the column.
DROP TABLE IF EXISTS proj_minmax_plain;
DROP TABLE IF EXISTS proj_minmax_commit_order;

CREATE TABLE proj_minmax_plain (id UInt64, v String)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, index_granularity = 4;
ALTER TABLE proj_minmax_plain ADD PROJECTION p (SELECT * ORDER BY id) WITH SETTINGS (add_minmax_index_for_block_number_column = 1);

CREATE TABLE proj_minmax_commit_order (id UInt64, v String)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1, index_granularity = 4;
ALTER TABLE proj_minmax_commit_order ADD PROJECTION p INDEX * TYPE commit_order WITH SETTINGS (add_minmax_index_for_block_number_column = 1);

INSERT INTO proj_minmax_plain SELECT number, toString(number) FROM numbers(10);
INSERT INTO proj_minmax_plain SELECT number + 10, toString(number) FROM numbers(10);
OPTIMIZE TABLE proj_minmax_plain FINAL;

INSERT INTO proj_minmax_commit_order SELECT number, toString(number) FROM numbers(10);
INSERT INTO proj_minmax_commit_order SELECT number + 10, toString(number) FROM numbers(10);
OPTIMIZE TABLE proj_minmax_commit_order FINAL;

SELECT 'minmax plain', count() FROM proj_minmax_plain WHERE id = 15 AND _block_number = 2;
SELECT 'minmax commit_order', count() FROM proj_minmax_commit_order WHERE id = 15 AND _block_number = 2;

DROP TABLE proj_minmax_plain;
DROP TABLE proj_minmax_commit_order;

-- A projection that stores only `_block_number`: it provides that column, but neither `_block_offset` nor part min-max index slots for block columns.
DROP TABLE IF EXISTS proj_partial;

CREATE TABLE proj_partial (id UInt64, v String, PROJECTION p (SELECT *, _block_number ORDER BY id))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    allow_commit_order_projection = 1,
    part_minmax_index_columns = 'with_block_number_offset',
    index_granularity = 4;

INSERT INTO proj_partial SELECT number, toString(number) FROM numbers(10);
INSERT INTO proj_partial SELECT number + 10, toString(number) FROM numbers(10);
OPTIMIZE TABLE proj_partial FINAL;

SELECT 'partial number read', id, _block_number FROM proj_partial WHERE id = 15 SETTINGS force_optimize_projection_name = 'p';
SELECT 'partial number filter', count() FROM proj_partial WHERE id = 15 AND _block_number = 2;
SELECT 'partial offset filter', count() FROM proj_partial WHERE id = 15 AND _block_offset = 5;

DROP TABLE proj_partial;

-- Incremental refresh over a source whose projection stores the block columns: the projection may be used and nothing is lost.
DROP TABLE IF EXISTS proj_stored_mv;
DROP TABLE IF EXISTS proj_stored_tgt;
DROP TABLE IF EXISTS proj_stored_src;

CREATE TABLE proj_stored_src (id UInt64, ts DateTime, v String, PROJECTION by_id (SELECT *, _block_number, _block_offset ORDER BY id))
ENGINE = MergeTree ORDER BY ts
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    allow_commit_order_projection = 1,
    part_minmax_index_columns = 'with_block_number_offset',
    index_granularity = 4;

CREATE TABLE proj_stored_tgt (id UInt64, ts DateTime, v String) ENGINE = MergeTree ORDER BY id;

CREATE MATERIALIZED VIEW proj_stored_mv
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL
    TO proj_stored_tgt EMPTY
    AS SELECT id, ts, v FROM proj_stored_src WHERE id > 12 AND id < 28;

INSERT INTO proj_stored_src SELECT number, toDateTime('2026-01-01 00:00:00') - number, toString(number) FROM numbers(10);
SYSTEM REFRESH VIEW proj_stored_mv;
SYSTEM WAIT VIEW proj_stored_mv;
SELECT 'stored round1', count() FROM proj_stored_tgt;

INSERT INTO proj_stored_src SELECT number + 10, toDateTime('2026-01-01 00:00:00') - number, toString(number) FROM numbers(10);
INSERT INTO proj_stored_src SELECT number + 20, toDateTime('2026-01-01 00:00:00') - number, toString(number) FROM numbers(10);
OPTIMIZE TABLE proj_stored_src FINAL;
SYSTEM REFRESH VIEW proj_stored_mv;
SYSTEM WAIT VIEW proj_stored_mv;
SELECT 'stored round2', count(), min(id), max(id) FROM proj_stored_tgt;

SYSTEM REFRESH VIEW proj_stored_mv;
SYSTEM WAIT VIEW proj_stored_mv;
SELECT 'stored round3', count() FROM proj_stored_tgt;

DROP TABLE proj_stored_mv;
DROP TABLE proj_stored_tgt;
DROP TABLE proj_stored_src;
