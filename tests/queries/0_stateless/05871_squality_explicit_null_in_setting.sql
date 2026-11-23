SET in_null_as_unknown = 1;

SELECT
    nullIn(CAST(NULL AS Nullable(UInt8)), (1, NULL)),
    notNullIn(CAST(NULL AS Nullable(UInt8)), (1, NULL)),
    globalNullIn(CAST(NULL AS Nullable(UInt8)), (1, NULL)),
    globalNotNullIn(CAST(NULL AS Nullable(UInt8)), (1, NULL));

SELECT
    toTypeName(nullIn(CAST(NULL AS Nullable(UInt8)), (1, NULL))),
    toTypeName(notNullIn(CAST(NULL AS Nullable(UInt8)), (1, NULL))),
    toTypeName(globalNullIn(CAST(NULL AS Nullable(UInt8)), (1, NULL))),
    toTypeName(globalNotNullIn(CAST(NULL AS Nullable(UInt8)), (1, NULL)));

SELECT
    nullIn(tuple(toUInt8(1), toUInt8(2)), (tuple(toUInt8(1), toUInt8(2)))),
    notNullIn(tuple(toUInt8(1), toUInt8(2)), (tuple(toUInt8(1), toUInt8(2))));

SELECT
    nullIn(CAST(NULL AS Nullable(UInt8)), (SELECT CAST(NULL AS Nullable(UInt8)))),
    notNullIn(CAST(NULL AS Nullable(UInt8)), (SELECT CAST(NULL AS Nullable(UInt8))));

SET transform_null_in = 1;
SELECT toTypeName(84 IN (63, NULL)), 84 IN (63, NULL), nullIn(84, (63, NULL)), toTypeName(nullIn(84, (63, NULL)));
SELECT (1, NULL) IN ((1, NULL)); -- { serverError NOT_IMPLEMENTED }

SET allow_experimental_analyzer = 1;
SELECT
    nullIn(toUInt8(number), (SELECT CAST(NULL AS Nullable(UInt8)))),
    toUInt8(number) IN (SELECT CAST(NULL AS Nullable(UInt8)))
FROM numbers(1);
SELECT
    toUInt8(number) IN (SELECT CAST(NULL AS Nullable(UInt8))),
    nullIn(toUInt8(number), (SELECT CAST(NULL AS Nullable(UInt8))))
FROM numbers(1);

SET allow_experimental_analyzer = 0;
SELECT toTypeName(84 IN (63, NULL)), 84 IN (63, NULL), nullIn(84, (63, NULL)), toTypeName(nullIn(84, (63, NULL)));
SELECT (1, NULL) IN ((1, NULL)); -- { serverError NOT_IMPLEMENTED }
