SET in_null_as_unknown = 1;

SELECT
    isNull([toUInt8(0)] IN (['256'], NULL)),
    isNull([toUInt8(0)] IN (materialize(['256']), NULL));

SELECT
    isNull(map('a', toUInt8(0)) IN (map('a', '256'), NULL)),
    isNull(map('a', toUInt8(0)) IN (materialize(map('a', '256')), NULL));

SELECT count() FROM values('a Array(UInt8)', ([0]))
WHERE a IN (materialize(['256']), NULL);

DROP TABLE IF EXISTS squality_typed_compound_string_check;
CREATE TABLE squality_typed_compound_string_check
(
    a Array(UInt8),
    b Array(String),
    CONSTRAINT c CHECK a IN (b, NULL)
) ENGINE = Memory;
INSERT INTO squality_typed_compound_string_check VALUES ([0], ['256']); -- { serverError VIOLATED_CONSTRAINT }
DROP TABLE squality_typed_compound_string_check;
