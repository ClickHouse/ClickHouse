-- Engine: Trino 476
CREATE SCHEMA IF NOT EXISTS warehouse.example_schema WITH (location = 's3a://warehouse/example');

CREATE TABLE warehouse.example_schema.dangling_deletes (c1 INTEGER, c2 INTEGER) WITH (
    format = 'PARQUET',
    location = 's3a://warehouse/dangling_deletes'
);

INSERT INTO
    warehouse.example_schema.dangling_deletes
VALUES
    (0, 12),
    (1, 123),
    (2, 2314),
    (3, 9),
    (4, 1292),
    (5, 12831),
    (6, 12381),
    (7, 123999);

INSERT INTO
    warehouse.example_schema.dangling_deletes
VALUES
    (8, 12318231),
    (9, 999),
    (10, 1010),
    (11, 11),
    (12, 1212),
    (13, 1313),
    (14, 91121),
    (15, 182222);

DELETE FROM
    warehouse.example_schema.dangling_deletes
WHERE
    c1 % 3 = 0;

ALTER TABLE warehouse.example_schema.dangling_deletes EXECUTE optimize WHERE "$path" = 's3a://warehouse/dangling_deletes/data/20250910_130933_00003_yek99-8428445f-b086-41b6-9e3a-5a47c052d0aa.parquet';

ALTER TABLE warehouse.example_schema.dangling_deletes EXECUTE optimize WHERE "$path" = 's3a://warehouse/dangling_deletes/data/20250910_130929_00002_yek99-a0e41fe0-dec9-4fcb-a053-5714c0a0cca0.parquet';

ALTER TABLE warehouse.example_schema.dangling_deletes EXECUTE optimize;

SELECT * FROM warehouse.example_schema."dangling_deletes$files";
