SET in_null_as_unknown = 0;

SELECT 84 IN (63, NULL), 84 NOT IN (63, NULL), NULL IN (NULL), NULL NOT IN (NULL), NULL IN (), NULL NOT IN ();

SET in_null_as_unknown = 1;
SET allow_suspicious_low_cardinality_types = 1;

SELECT toTypeName(84 IN (63, NULL)), 84 IN (63, NULL), 84 NOT IN (63, NULL);
SELECT 63 IN (63, NULL), 63 NOT IN (63, NULL);
SELECT NULL IN (63), NULL NOT IN (63);
SELECT NULL IN (), NULL NOT IN ();
SELECT 84 IN (SELECT * FROM VALUES('value Nullable(Int64)', (63), (NULL)));
SELECT 84 NOT IN (SELECT * FROM VALUES('value Nullable(Int64)', (63), (NULL)));
SELECT 63 IN (SELECT * FROM VALUES('value Nullable(Int64)', (63), (NULL)));

WITH CAST(NULL AS LowCardinality(Nullable(Int64))) AS value
SELECT value IN (1), value NOT IN (1);

SET transform_null_in = 1;
SELECT NULL IN (NULL), NULL NOT IN (NULL);

SET allow_experimental_analyzer = 0;
SELECT 84 IN (63, NULL), 84 NOT IN (63, NULL);
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
