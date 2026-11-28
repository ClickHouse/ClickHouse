-- `arrayExists(x -> x = c, arr)` must not be rewritten to `has(arr, c)` when `has` does not compare
-- the element numerically: an `Array(BFloat16)` falls to the generic column comparison and an
-- `Array(LowCardinality(Float64))` is searched by dictionary lookup, both of which match a stored NaN,
-- while `equals` does not.

DROP TABLE IF EXISTS t_bfloat16;
CREATE TABLE t_bfloat16 (a Array(BFloat16)) ENGINE = Memory;
INSERT INTO t_bfloat16 VALUES ([nan]);

SELECT arrayExists(x -> x = toBFloat16(nan), a) FROM t_bfloat16 SETTINGS optimize_rewrite_array_exists_to_has = 1;
SELECT arrayExists(x -> x = toBFloat16(nan), a) FROM t_bfloat16 SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT count() FROM t_bfloat16 WHERE arrayExists(x -> x = toBFloat16(nan), a) SETTINGS optimize_rewrite_array_exists_to_has = 1;
SELECT count() FROM t_bfloat16 WHERE arrayExists(x -> x = toBFloat16(nan), a) SETTINGS optimize_rewrite_array_exists_to_has = 0;

DROP TABLE t_bfloat16;

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_lc_float;
CREATE TABLE t_lc_float (a Array(LowCardinality(Float64))) ENGINE = Memory;
INSERT INTO t_lc_float VALUES ([nan]);

SELECT arrayExists(x -> x = nan, a) FROM t_lc_float SETTINGS optimize_rewrite_array_exists_to_has = 1;
SELECT arrayExists(x -> x = nan, a) FROM t_lc_float SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT count() FROM t_lc_float WHERE arrayExists(x -> x = nan, a) SETTINGS optimize_rewrite_array_exists_to_has = 1;
SELECT count() FROM t_lc_float WHERE arrayExists(x -> x = nan, a) SETTINGS optimize_rewrite_array_exists_to_has = 0;

DROP TABLE t_lc_float;
