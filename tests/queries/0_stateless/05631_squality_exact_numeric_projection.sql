DROP TABLE IF EXISTS squality_exact_numeric_projection;
CREATE TABLE squality_exact_numeric_projection
(
    d Decimal(18, 0),
    x Float64,
    PROJECTION p (SELECT count() WHERE d = x)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO squality_exact_numeric_projection SETTINGS allow_lossy_numeric_supertype = 1
VALUES ('9007199254740993', 9007199254740992.);

SELECT count() FROM squality_exact_numeric_projection WHERE d = x
SETTINGS allow_lossy_numeric_supertype = 0, optimize_use_projections = 0, optimize_trivial_count_query = 0;
SELECT count() FROM squality_exact_numeric_projection WHERE d = x
SETTINGS allow_lossy_numeric_supertype = 0, optimize_use_projections = 1,
    force_optimize_projection = 1, optimize_trivial_count_query = 0;
SELECT count() FROM squality_exact_numeric_projection WHERE d = x
SETTINGS allow_lossy_numeric_supertype = 1, optimize_use_projections = 0, optimize_trivial_count_query = 0;
SELECT count() FROM squality_exact_numeric_projection WHERE d = x
SETTINGS allow_lossy_numeric_supertype = 1, optimize_use_projections = 1, optimize_trivial_count_query = 0;

DROP TABLE squality_exact_numeric_projection;

DROP TABLE IF EXISTS squality_exact_numeric_group_projection;
CREATE TABLE squality_exact_numeric_group_projection
(
    d Decimal(18, 0),
    x Float64,
    PROJECTION p (SELECT d = x AS m, count() GROUP BY m)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO squality_exact_numeric_group_projection
VALUES ('9007199254740993', 9007199254740992.);

SELECT d = x AS m, count() FROM squality_exact_numeric_group_projection GROUP BY m
SETTINGS allow_lossy_numeric_supertype = 0, optimize_use_projections = 1,
    force_optimize_projection = 1;
SELECT d = x AS m, count() FROM squality_exact_numeric_group_projection GROUP BY m
SETTINGS allow_lossy_numeric_supertype = 1, optimize_use_projections = 0;
SELECT d = x AS m, count() FROM squality_exact_numeric_group_projection GROUP BY m
SETTINGS allow_lossy_numeric_supertype = 1, optimize_use_projections = 1;

DROP TABLE squality_exact_numeric_group_projection;

DROP TABLE IF EXISTS squality_exact_numeric_null_safe_projection;
CREATE TABLE squality_exact_numeric_null_safe_projection
(
    d Decimal(18, 0),
    x Float64,
    PROJECTION p (SELECT count() WHERE isNotDistinctFrom(d, x))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO squality_exact_numeric_null_safe_projection
VALUES ('9007199254740993', 9007199254740992.);

SELECT count() FROM squality_exact_numeric_null_safe_projection WHERE isNotDistinctFrom(d, x)
SETTINGS allow_lossy_numeric_supertype = 0, optimize_use_projections = 1,
    force_optimize_projection = 1, optimize_trivial_count_query = 0;
SELECT count() FROM squality_exact_numeric_null_safe_projection WHERE isNotDistinctFrom(d, x)
SETTINGS allow_lossy_numeric_supertype = 1, optimize_use_projections = 0, optimize_trivial_count_query = 0;
SELECT count() FROM squality_exact_numeric_null_safe_projection WHERE isNotDistinctFrom(d, x)
SETTINGS allow_lossy_numeric_supertype = 1, optimize_use_projections = 1, optimize_trivial_count_query = 0;

DROP TABLE squality_exact_numeric_null_safe_projection;
