-- Tags: no-parallel-replicas
-- A space-filling curve whose first argument is a range-mask column, e.g. `mortonEncode(mask, x)` with
-- `mask Tuple(UInt8)`, is a one-dimensional curve over `x`. The index analysis must not treat it as a
-- two-dimensional curve over (mask, x): that dropped matching rows and threw `BAD_TYPE_OF_FIELD`.
-- A curve over `Dynamic` arguments can get values >= 2^32, which the analysis dropped as well.

SET analyze_index_with_space_filling_curves = 1;
SET use_primary_key = 1;
SET use_skip_indexes = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_curve_mask_pk;
DROP TABLE IF EXISTS t_curve_mask_skip;
DROP TABLE IF EXISTS t_curve_mask_partition;
DROP TABLE IF EXISTS t_curve_dynamic;

CREATE TABLE t_curve_mask_pk (mask Tuple(UInt8), x UInt32)
ENGINE = MergeTree ORDER BY mortonEncode(mask, x)
SETTINGS index_granularity = 1;
INSERT INTO t_curve_mask_pk VALUES ((1), 1), ((1), 2), ((1), 3);
SELECT x FROM t_curve_mask_pk WHERE x >= 2 ORDER BY x;
SELECT count() FROM t_curve_mask_pk WHERE mask = tuple(1);
SELECT countIf(explain ILIKE '%has args in%') = 0
FROM (EXPLAIN indexes = 1 SELECT x FROM t_curve_mask_pk WHERE x >= 2);

CREATE TABLE t_curve_mask_skip (mask Tuple(UInt8), x UInt32, INDEX i hilbertEncode(mask, x) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;
INSERT INTO t_curve_mask_skip VALUES ((0), 1), ((0), 2), ((0), 3);
SELECT x FROM t_curve_mask_skip WHERE x >= 2 ORDER BY x;
SELECT count() FROM t_curve_mask_skip WHERE mask = tuple(0);

CREATE TABLE t_curve_mask_partition (mask Tuple(UInt8), x UInt32)
ENGINE = MergeTree PARTITION BY mortonEncode(mask, x) ORDER BY tuple();
INSERT INTO t_curve_mask_partition VALUES ((1), 1), ((1), 2), ((1), 3);
SELECT x FROM t_curve_mask_partition WHERE x >= 2 ORDER BY x;
SELECT count() FROM t_curve_mask_partition WHERE mask = tuple(1);

CREATE TABLE t_curve_dynamic (x Dynamic, y Dynamic, INDEX i mortonEncode(x, y) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_curve_dynamic SELECT 4294967296::UInt64, 0::UInt64;
SELECT count() FROM t_curve_dynamic WHERE x >= 4294967296 AND y = 0;
SELECT count() FROM t_curve_dynamic WHERE x >= 4294967296 AND y = 0 SETTINGS analyze_index_with_space_filling_curves = 0;

DROP TABLE t_curve_mask_pk;
DROP TABLE t_curve_mask_skip;
DROP TABLE t_curve_mask_partition;
DROP TABLE t_curve_dynamic;
