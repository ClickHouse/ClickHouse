SET in_null_as_unknown = 1;

SELECT isNull([toUInt64(18446744073709551615)] IN
    ([CAST('neg' AS Enum8('neg' = -1))], NULL));
SELECT isNull([toUInt64(18446744073709551615)] IN
    (materialize([CAST('neg' AS Enum8('neg' = -1))]), NULL));

SELECT count() FROM
(
    SELECT [toUInt64(18446744073709551615)] AS x,
        materialize([CAST('neg' AS Enum8('neg' = -1))]) AS e
)
WHERE x IN (e, NULL);

CREATE TABLE squality_compound_enum_check
(
    x Array(UInt64),
    e Array(Enum8('neg' = -1)),
    CONSTRAINT no_false_match CHECK x NOT IN (materialize(e), NULL)
) ENGINE = Memory;
INSERT INTO squality_compound_enum_check VALUES ([18446744073709551615], [-1]);
SELECT count() FROM squality_compound_enum_check;
DROP TABLE squality_compound_enum_check;
