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

SELECT (1, NULL) IN ((1, NULL)); -- { serverError NOT_IMPLEMENTED }
