-- Tags: shard

SET in_null_as_unknown = 0;

SELECT 84 IN (63, NULL), 84 NOT IN (63, NULL), NULL IN (NULL), NULL NOT IN (NULL), NULL IN (), NULL NOT IN ();

SET in_null_as_unknown = 1;
SET allow_suspicious_low_cardinality_types = 1;

SELECT toTypeName(84 IN (63, NULL)), 84 IN (63, NULL), 84 NOT IN (63, NULL);
SELECT 63 IN (63, NULL), 63 NOT IN (63, NULL);
SELECT groupArray(number) FROM numbers(3) WHERE number NOT IN (1);
SELECT groupArray(NOT has([1], number)) FROM numbers(2);
SELECT materialize(NULL) IN (1), materialize(NULL) NOT IN (1), materialize(NULL) IN (), materialize(NULL) NOT IN ();
SELECT CAST(NULL AS Nullable(UInt8)) IN (256), CAST(NULL AS Nullable(UInt8)) NOT IN (256);
SELECT value, value IN (256), value NOT IN (256)
FROM VALUES('value Nullable(UInt8)', (NULL), (1))
ORDER BY value NULLS FIRST;
SELECT NULL IN (63), NULL NOT IN (63);
SELECT NULL IN (), NULL NOT IN ();
SELECT 84 IN (SELECT * FROM VALUES('value Nullable(Int64)', (63), (NULL)));
SELECT 84 NOT IN (SELECT * FROM VALUES('value Nullable(Int64)', (63), (NULL)));
SELECT 63 IN (SELECT * FROM VALUES('value Nullable(Int64)', (63), (NULL)));
SET allow_experimental_variant_type = 1;
SELECT
    toUInt64(84) IN (SELECT value FROM VALUES('value Variant(UInt64, String)', (NULL))),
    toUInt64(84) NOT IN (SELECT value FROM VALUES('value Variant(UInt64, String)', (NULL)));
SELECT
    toUInt64(84) IN (SELECT value FROM VALUES('value Dynamic', (NULL))),
    toUInt64(84) NOT IN (SELECT value FROM VALUES('value Dynamic', (NULL)));
SELECT
    CAST(toUInt64(84), 'Variant(UInt64, String)') IN materialize([CAST(NULL, 'Variant(UInt64, String)')]),
    CAST(toUInt64(84), 'Variant(UInt64, String)') NOT IN materialize([CAST(NULL, 'Variant(UInt64, String)')]);
SELECT
    CAST(toUInt64(84), 'Dynamic') IN materialize([CAST(NULL, 'Dynamic')]),
    CAST(toUInt64(84), 'Dynamic') NOT IN materialize([CAST(NULL, 'Dynamic')]);

WITH CAST(NULL AS LowCardinality(Nullable(Int64))) AS value
SELECT value IN (1), value NOT IN (1);

SET transform_null_in = 1;
SELECT NULL IN (NULL), NULL NOT IN (NULL);
SELECT NULL IN (number, NULL), NULL NOT IN (number, NULL) FROM numbers(1);

SET transform_null_in = 0;
SELECT NULL IN materialize(tuple()), NULL NOT IN materialize(tuple());
SET transform_null_in = 1;

SET allow_experimental_analyzer = 0;
SELECT 84 IN (63, NULL), 84 NOT IN (63, NULL);
SELECT value, value IN (256), value NOT IN (256)
FROM VALUES('value Nullable(UInt8)', (NULL), (1))
ORDER BY value NULLS FIRST;
SELECT NULL IN (63), NULL NOT IN (63);
SELECT NULL IN (), NULL NOT IN ();
SELECT
    toTypeName(number IN materialize(toNullable(number + 1))),
    number IN materialize(toNullable(number + 1)),
    number IN materialize(if(number = 0, NULL, number + 1))
FROM numbers(2);
SELECT
    84 IN materialize([toNullable(63), NULL]),
    63 IN materialize([toNullable(63), NULL]),
    NULL IN materialize([toNullable(63)]),
    NULL IN materialize([]),
    toTypeName(84 IN materialize([63]));
SET allow_experimental_analyzer = 1;

SELECT
    toTypeName(number IN materialize(toNullable(number + 1))),
    number IN materialize(toNullable(number + 1)),
    number IN materialize(if(number = 0, NULL, number + 1))
FROM numbers(2);
SELECT
    84 IN materialize([toNullable(63), NULL]),
    63 IN materialize([toNullable(63), NULL]),
    NULL IN materialize([toNullable(63)]),
    NULL IN materialize([]),
    toTypeName(84 IN materialize([63]));

SELECT [1] IN (NULL), [1] NOT IN ([2], NULL);
SELECT map('a', 1) IN (NULL), map('a', 1) NOT IN (map('b', 2), NULL);

SET allow_experimental_analyzer = 0;
SELECT materialize(NULL) IN emptyArrayUInt8(), materialize(NULL) NOT IN emptyArrayUInt8();
SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS squality_in_null_serialized;
CREATE TABLE squality_in_null_serialized (k Nullable(UInt8)) ENGINE = Memory;
INSERT INTO squality_in_null_serialized VALUES (NULL), (1);

SELECT isNull(k), k NOT IN (256)
FROM squality_in_null_serialized
ORDER BY isNull(k) DESC;

SELECT isNull(k), k NOT IN (256)
FROM remote('127.0.0.1', currentDatabase(), squality_in_null_serialized)
ORDER BY isNull(k) DESC
SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

DROP TABLE squality_in_null_serialized;

SELECT (1, NULL) IN ((1, NULL)); -- { serverError NOT_IMPLEMENTED }
