-- Tags: no-fasttest

-- The numbers of an array (and of the tuples of an array) that have the type of their column are copied from the
-- decoder's staging area a whole array at a time. Empty arrays, arrays around the sizes at which a block asks for its
-- room at once, and large ones must all read back as written, and so must the types that are converted value by value.

DROP VIEW IF EXISTS source_05573;
CREATE VIEW source_05573 AS
SELECT
    toUInt32(number) AS id,
    arrayMap(i -> toInt64(number * 1000003 - i * 7919), range(len)) AS longs,
    arrayMap(i -> toInt32(number - i), range(len)) AS ints,
    arrayMap(i -> toFloat64(i) / 4 - number, range(len)) AS doubles,
    arrayMap(i -> toFloat32(i / 2), range(len)) AS floats,
    arrayMap(i -> toBool((i + number) % 3 = 0), range(len)) AS flags,
    arrayMap(i -> CAST(tuple(toInt64(i * number), toFloat64(i) / 8, toInt32(number + i), toFloat32(i) / 4, toBool(i % 2 = 0)),
        'Tuple(a Int64, b Float64, c Int32, d Float32, e Bool)'), range(len)) AS tuples
FROM (SELECT number, [0, 1, 15, 16, 17, 100, 1000][number % 7 + 1] AS len FROM numbers(21));

INSERT INTO FUNCTION file(currentDatabase() || '_data_05573.avro', 'Avro',
    'id UInt32, longs Array(Int64), ints Array(Int32), doubles Array(Float64), floats Array(Float32), flags Array(Bool), tuples Array(Tuple(a Int64, b Float64, c Int32, d Float32, e Bool))')
SELECT * FROM source_05573 SETTINGS engine_file_truncate_on_insert = 1;

SELECT count(), sum(length(longs)), sum(length(tuples)) FROM file(currentDatabase() || '_data_05573.avro', 'Avro',
    'id UInt32, longs Array(Int64), tuples Array(Tuple(a Int64, b Float64, c Int32, d Float32, e Bool))');

SELECT 'same types',
    (SELECT sum(cityHash64(*)) FROM file(currentDatabase() || '_data_05573.avro', 'Avro',
        'id UInt32, longs Array(Int64), ints Array(Int32), doubles Array(Float64), floats Array(Float32), flags Array(Bool), tuples Array(Tuple(a Int64, b Float64, c Int32, d Float32, e Bool))'))
    = (SELECT sum(cityHash64(*)) FROM source_05573);

SELECT 'tuple elements in another order',
    (SELECT sum(cityHash64(tuples)) FROM file(currentDatabase() || '_data_05573.avro', 'Avro',
        'id UInt32, tuples Array(Tuple(e Bool, d Float32, c Int32, b Float64, a Int64))'))
    = (SELECT sum(cityHash64(arrayMap(t -> (t.e, t.d, t.c, t.b, t.a), tuples))) FROM source_05573);

SELECT 'a subset of the columns',
    (SELECT sum(cityHash64(ints, floats)) FROM file(currentDatabase() || '_data_05573.avro', 'Avro', 'ints Array(Int32), floats Array(Float32)'))
    = (SELECT sum(cityHash64(ints, floats)) FROM source_05573);

SELECT 'converted types',
    (SELECT sum(cityHash64(*)) FROM file(currentDatabase() || '_data_05573.avro', 'Avro',
        'id UInt32, longs Array(Nullable(Int64)), ints Array(Int64), doubles Array(Float64), floats Array(Float64), flags Array(UInt8)'))
    = (SELECT sum(cityHash64(*)) FROM (SELECT id, CAST(longs AS Array(Nullable(Int64))) AS longs, CAST(ints AS Array(Int64)) AS ints,
        doubles, CAST(floats AS Array(Float64)) AS floats, CAST(flags AS Array(UInt8)) AS flags FROM source_05573));

SELECT 'converted tuple elements',
    (SELECT sum(cityHash64(tuples)) FROM file(currentDatabase() || '_data_05573.avro', 'Avro',
        'tuples Array(Tuple(a Nullable(Int64), b Float64, c Int64, d Float64, e UInt8))'))
    = (SELECT sum(cityHash64(arrayMap(t -> (CAST(t.a AS Nullable(Int64)), t.b, CAST(t.c AS Int64), CAST(t.d AS Float64), CAST(t.e AS UInt8)), tuples))) FROM source_05573);

DROP VIEW source_05573;
