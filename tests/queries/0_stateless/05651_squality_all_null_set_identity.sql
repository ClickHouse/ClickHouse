SET in_null_as_unknown = 1;

SELECT materialize(NULL) IN (1), toUInt8(number) IN (1)
FROM numbers(2);

SELECT toUInt8(number) IN (1), materialize(NULL) IN (1)
FROM numbers(2);

SELECT count() FROM numbers(2)
WHERE (materialize(NULL) IN (1)) IS NULL AND toUInt8(number) IN (1);

SELECT materialize(NULL) NOT IN (1), materialize(NULL) IN [];

DROP TABLE IF EXISTS squality_all_null_set_check;
CREATE TABLE squality_all_null_set_check
(
    x UInt8,
    CONSTRAINT c CHECK (materialize(NULL) IN (1)) IS NULL AND x IN (1)
) ENGINE = Memory;
INSERT INTO squality_all_null_set_check VALUES (1);
INSERT INTO squality_all_null_set_check VALUES (0); -- { serverError VIOLATED_CONSTRAINT }
SELECT x FROM squality_all_null_set_check;
DROP TABLE squality_all_null_set_check;
