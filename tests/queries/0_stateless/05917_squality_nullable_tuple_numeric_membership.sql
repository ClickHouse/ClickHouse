SET in_null_as_unknown = 1;

SELECT
    [CAST(NULL AS Nullable(Tuple(Nullable(UInt8))))]
        IN [tuple(CAST(NULL AS Nullable(UInt16)))],
    [CAST(NULL AS Nullable(Tuple(Nullable(UInt8))))]
        IN materialize([tuple(CAST(NULL AS Nullable(UInt16)))]);

SELECT count() FROM numbers(1)
WHERE [CAST(NULL AS Nullable(Tuple(Nullable(UInt8))))]
    IN [tuple(CAST(NULL AS Nullable(UInt16)))];
SELECT count() FROM numbers(1)
WHERE [CAST(NULL AS Nullable(Tuple(Nullable(UInt8))))]
    IN materialize([tuple(CAST(NULL AS Nullable(UInt16)))]);

SELECT [CAST(NULL AS Nullable(Tuple(Nullable(UInt8))))]
    IN materialize([CAST(NULL AS Nullable(Tuple(Nullable(UInt16))))]);

CREATE TABLE squality_nullable_tuple_numeric_check
(
    l Array(Nullable(Tuple(Nullable(UInt8)))),
    r Array(Tuple(Nullable(UInt16))),
    CONSTRAINT null_tuple_is_not_member CHECK NOT (l IN materialize(r))
) ENGINE = Memory;

INSERT INTO squality_nullable_tuple_numeric_check
SELECT [CAST(NULL AS Nullable(Tuple(Nullable(UInt8))))],
    [tuple(CAST(NULL AS Nullable(UInt16)))];
SELECT count() FROM squality_nullable_tuple_numeric_check;
DROP TABLE squality_nullable_tuple_numeric_check;
