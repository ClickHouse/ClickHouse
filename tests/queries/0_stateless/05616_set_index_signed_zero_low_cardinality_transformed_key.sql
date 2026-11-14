-- A `LowCardinality(Float64)` column keeps one dictionary entry for `-0.` and `0.`, so the two spellings
-- cannot be probed through the key transform in the column's own type. The sorting key of a stored row
-- can still be computed from `-0.`, so an `IN` set with a zero must not prune by such a transformed key.
-- The ground truth is taken with `countIf`, which evaluates the predicate on every row.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_signed_zero_lc;

CREATE TABLE t_signed_zero_lc (f LowCardinality(Float64)) ENGINE = MergeTree ORDER BY reinterpretAsUInt64(f)
    SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_lc VALUES (-0.), (1.), (2.);

SELECT count() FROM t_signed_zero_lc WHERE f IN (-0., 999.);
SELECT countIf(f IN (-0., 999.)) FROM t_signed_zero_lc;
SELECT count() FROM t_signed_zero_lc WHERE f IN (0., 999.);
SELECT countIf(f IN (0., 999.)) FROM t_signed_zero_lc;
SELECT count() FROM t_signed_zero_lc WHERE f IN (SELECT -0.);
SELECT countIf(f IN (SELECT -0.)) FROM t_signed_zero_lc;

-- A set without a zero still prunes. Depending on the insert path, the sorting key of the stored zero row
-- is computed from either spelling, so it sorts either first or last; the probed values are kept away
-- from both ends, so that pruning does not depend on where the zero row lands.
INSERT INTO t_signed_zero_lc SELECT number + 3 FROM numbers(20);
SELECT count() FROM t_signed_zero_lc WHERE f IN (10., 11.) SETTINGS max_rows_to_read = 6;

DROP TABLE t_signed_zero_lc;
