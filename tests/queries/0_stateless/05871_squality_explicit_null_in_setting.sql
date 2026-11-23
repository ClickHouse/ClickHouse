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
