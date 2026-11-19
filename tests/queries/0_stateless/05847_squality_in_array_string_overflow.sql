SET in_null_as_unknown = 1;

SELECT
    isNull([toUInt8(0)] IN ('[256]', NULL)),
    isNull([toUInt8(0)] IN (materialize('[256]'), NULL)),
    [toUInt8(0)] IN materialize('[256]');

SELECT count() FROM values('a Array(UInt8)', ([0]))
WHERE a IN (materialize('[256]'), NULL);

DROP TABLE IF EXISTS squality_array_string_overflow_check;
CREATE TABLE squality_array_string_overflow_check
(
    a Array(UInt8),
    s String,
    CONSTRAINT c CHECK a IN (s, NULL)
) ENGINE = Memory;

INSERT INTO squality_array_string_overflow_check VALUES ([0], '[256]'); -- { serverError VIOLATED_CONSTRAINT }

DROP TABLE squality_array_string_overflow_check;
