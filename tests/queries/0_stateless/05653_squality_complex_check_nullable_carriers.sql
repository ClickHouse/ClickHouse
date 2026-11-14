SET in_null_as_unknown = 1;

DROP TABLE IF EXISTS squality_complex_array_check;
CREATE TABLE squality_complex_array_check
(
    a Array(UInt8),
    v Nullable(String),
    CONSTRAINT c CHECK a IN ([1], v)
) ENGINE = Memory;
INSERT INTO squality_complex_array_check VALUES ([1], NULL);
SELECT a IN ([1], v) FROM squality_complex_array_check;
DROP TABLE squality_complex_array_check;

DROP TABLE IF EXISTS squality_complex_uuid_check;
CREATE TABLE squality_complex_uuid_check
(
    a Array(UInt8),
    v Nullable(UUID),
    CONSTRAINT c CHECK a IN ([1], v)
) ENGINE = Memory;
INSERT INTO squality_complex_uuid_check VALUES ([1], NULL);
SELECT a IN ([1], v) FROM squality_complex_uuid_check;
DROP TABLE squality_complex_uuid_check;

DROP TABLE IF EXISTS squality_complex_map_check;
CREATE TABLE squality_complex_map_check
(
    m Map(String, UInt8),
    v Nullable(String),
    CONSTRAINT c CHECK m IN (map('a', toUInt8(1)), v)
) ENGINE = Memory;
INSERT INTO squality_complex_map_check VALUES (map('a', toUInt8(1)), NULL);
SELECT m IN (map('a', toUInt8(1)), v) FROM squality_complex_map_check;
DROP TABLE squality_complex_map_check;
