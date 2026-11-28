SET in_null_as_unknown = 1;

SELECT [1] IN CAST(NULL AS Nullable(UInt8)), [1] NOT IN CAST(NULL AS Nullable(UInt8));
SELECT materialize(emptyArrayUInt8()) IN ([]), materialize(emptyArrayUInt8()) NOT IN ([]);

SET allow_experimental_analyzer = 0;
SELECT [1] IN CAST(NULL AS Nullable(UInt8)), [1] NOT IN CAST(NULL AS Nullable(UInt8));
SELECT materialize(emptyArrayUInt8()) IN ([]), materialize(emptyArrayUInt8()) NOT IN ([]);
SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS squality_in_null_projection;
CREATE TABLE squality_in_null_projection
(
    x UInt8,
    PROJECTION p (SELECT count() WHERE x NOT IN (1, NULL))
)
ENGINE = MergeTree ORDER BY x;

INSERT INTO squality_in_null_projection SETTINGS in_null_as_unknown = 1 VALUES (2);
SET in_null_as_unknown = 0;
SELECT count() FROM squality_in_null_projection WHERE x NOT IN (1, NULL)
SETTINGS optimize_use_projections = 0, optimize_trivial_count_query = 0;
SELECT count() FROM squality_in_null_projection WHERE x NOT IN (1, NULL)
SETTINGS optimize_use_projections = 1, optimize_trivial_count_query = 0;

DROP TABLE squality_in_null_projection;

SET in_null_as_unknown = 1;
DROP TABLE IF EXISTS squality_in_null_set_hash;
CREATE TABLE squality_in_null_set_hash
(
    a Array(UInt8),
    PROJECTION p (SELECT a NOT IN ([2]) AS m, count() GROUP BY m)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO squality_in_null_set_hash VALUES ([1]), ([2]);
SELECT a NOT IN ([2], NULL) AS m, count() FROM squality_in_null_set_hash GROUP BY m ORDER BY m NULLS FIRST
SETTINGS optimize_use_projections = 0;
SELECT a NOT IN ([2], NULL) AS m, count() FROM squality_in_null_set_hash GROUP BY m ORDER BY m NULLS FIRST
SETTINGS optimize_use_projections = 1;

DROP TABLE squality_in_null_set_hash;
