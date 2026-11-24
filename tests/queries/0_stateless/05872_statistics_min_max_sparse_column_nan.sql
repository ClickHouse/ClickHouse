-- The extremes of a sparse float column without default rows skip `NaN` like the ones of a full column,
-- so `min` and `max` answered from the statistics built from it are the same as the ones read from the rows.

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS src_nan;
DROP TABLE IF EXISTS dst_nan;

SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET use_statistics_for_min_max_aggregation = 1;
SET materialize_statistics_on_insert = 1;
SET optimize_aggregation_in_order = 0, force_aggregation_in_order = 0;
SET aggregate_functions_null_for_empty = 0;

-- Zeros are the default value, so the columns of `src` are stored sparse.
CREATE TABLE src (f Float64, g Float32) ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, min_bytes_for_wide_part = 0;
CREATE TABLE dst (f Float64, g Float32) ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2', materialize_statistics_on_merge = 1;

INSERT INTO src SELECT multiIf(number = 0, 1., number = 1, nan, number = 2, 5., 0.) AS f, f FROM numbers(1000);
-- The block inserted into `dst` is sparse and has no default rows: [1, nan, 5].
INSERT INTO dst SELECT f, g FROM src WHERE f != 0;

SELECT min(f), max(f), min(g), max(g), count() FROM dst;
SELECT min(f), max(f), min(g), max(g), count() FROM dst SETTINGS use_statistics_for_min_max_aggregation = 0;
-- The extremes of the same sparse block in the output.
SELECT f, g FROM src WHERE f != 0 SETTINGS extremes = 1;

-- Every value is `NaN`: both extremes are `NaN`.
CREATE TABLE src_nan (f Float64, g Float32) ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, min_bytes_for_wide_part = 0;
CREATE TABLE dst_nan (f Float64, g Float32) ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2', materialize_statistics_on_merge = 1;

INSERT INTO src_nan SELECT multiIf(number < 2, nan, 0.) AS f, f FROM numbers(1000);
INSERT INTO dst_nan SELECT f, g FROM src_nan WHERE f != 0;

SELECT min(f), max(f), min(g), max(g), count() FROM dst_nan;
SELECT min(f), max(f), min(g), max(g), count() FROM dst_nan SETTINGS use_statistics_for_min_max_aggregation = 0;

DROP TABLE src;
DROP TABLE dst;
DROP TABLE src_nan;
DROP TABLE dst_nan;
