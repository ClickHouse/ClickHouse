SET in_null_as_unknown = 1;

SELECT isNull(toUInt8(255) IN
    (CAST('neg' AS Enum8('neg' = -1)), CAST(toUInt8(1) AS Dynamic), NULL));
SELECT isNull(toUInt8(255) IN
    (materialize(CAST('neg' AS Enum8('neg' = -1))),
     materialize(CAST(toUInt8(1) AS Dynamic)), NULL));

SELECT count() FROM
(
    SELECT toUInt8(255) AS x,
        materialize(CAST('neg' AS Enum8('neg' = -1))) AS e,
        materialize(CAST(toUInt8(1) AS Dynamic)) AS d
)
WHERE x IN (e, d, NULL);
