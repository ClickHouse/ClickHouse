-- Tags: no-parallel-replicas

-- A `LIMIT 0` over a MergeTree read skips the analysis of its ranges, and keeps the checks that do not depend on them.

-- Pinned to the default: prefer_optimize_projection=1 (from the settings randomizer) takes a projection
-- regardless of cost and defeats the LIMIT-0 range-analysis skip this test asserts. The one query below
-- that explicitly wants it keeps its per-query SETTINGS override.
SET prefer_optimize_projection = 0;

DROP VIEW IF EXISTS vs;
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS tp;
DROP TABLE IF EXISTS tr;

CREATE TABLE t (k UInt64, flag UInt8, v UInt64, INDEX iv v TYPE minmax GRANULARITY 1)
ENGINE = MergeTree PARTITION BY k % 2 ORDER BY (k, flag) SETTINGS index_granularity = 64;
INSERT INTO t SELECT intDiv(number, 1000), number % 2, number % 100 FROM numbers(100000);

CREATE TABLE tp (k UInt64, v UInt64, PROJECTION p (SELECT * ORDER BY v))
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;
INSERT INTO tp SELECT number, number % 1000 FROM numbers(100000);

CREATE TABLE tr (k UInt64, v UInt64) ENGINE = ReplacingMergeTree ORDER BY k;
INSERT INTO tr SELECT number, 1 FROM numbers(100);
INSERT INTO tr SELECT number, 2 FROM numbers(50, 100);

-- A sealed view: its plan is optimized on its own, and cloned to build the set below.
CREATE VIEW vs DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT k FROM t WHERE flag LIMIT 0;

-- { echoOn }

SELECT count() FROM (EXPLAIN indexes = 1 SELECT k FROM t WHERE flag LIMIT 0) WHERE explain ILIKE '%PrimaryKey%';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT k FROM t WHERE flag LIMIT 1) WHERE explain ILIKE '%PrimaryKey%';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT k FROM t WHERE flag LIMIT 0 SETTINGS exact_rows_before_limit = 1) WHERE explain ILIKE '%PrimaryKey%';

SELECT k FROM t WHERE flag LIMIT 0 SETTINGS exact_rows_before_limit = 1, output_format_write_statistics = 0 FORMAT JSONCompact;

SELECT k FROM t WHERE v = 1 LIMIT 0 SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT k FROM t WHERE flag LIMIT 0 SETTINGS force_index_by_date = 1; -- { serverError INDEX_NOT_USED }
SELECT k FROM t WHERE flag LIMIT 0 SETTINGS force_data_skipping_indices = 'iv'; -- { serverError INDEX_NOT_USED }

SET use_query_condition_cache = 1;
SELECT k FROM t WHERE k < 10 AND flag AND v < 50 LIMIT 0;
SELECT count() FROM t WHERE k < 10 AND flag AND v < 50;

SELECT k FROM tp WHERE v = 7 LIMIT 0 SETTINGS optimize_use_projections = 1, force_optimize_projection_name = 'p';
SELECT k FROM tp WHERE v = 7 LIMIT 0 SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT count() FROM (EXPLAIN SELECT k FROM tp WHERE v = 7 LIMIT 0 SETTINGS optimize_use_projections = 1, prefer_optimize_projection = 1) WHERE explain ILIKE '%ReadFromMergeTree (p)%';

SELECT k, v FROM tr FINAL WHERE k > 10 LIMIT 0 FORMAT TSVWithNamesAndTypes;

SELECT k FROM t WHERE flag LIMIT 0 SETTINGS max_rows_to_read = 10;
SELECT k FROM t WHERE flag LIMIT 1 SETTINGS max_rows_to_read = 10 FORMAT Null; -- { serverError TOO_MANY_ROWS }

SELECT count() FROM t WHERE k IN (SELECT k FROM vs) SETTINGS max_rows_to_read = 10;

-- { echoOff }

DROP VIEW vs;
DROP TABLE t;
DROP TABLE tp;
DROP TABLE tr;
