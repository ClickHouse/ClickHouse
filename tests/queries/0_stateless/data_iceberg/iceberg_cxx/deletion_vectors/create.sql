-- Engine: Trino 480
CREATE SCHEMA IF NOT EXISTS warehouse.example_schema WITH (location = 's3a://warehouse/example');

CREATE TABLE warehouse.example_schema.deletion_vectors (c1 INTEGER, c2 INTEGER) WITH (
    format = 'PARQUET',
    location = 's3a://warehouse/deletion_vectors',
    format_version = 3
);

INSERT INTO
    warehouse.example_schema.deletion_vectors
VALUES
    (0, 12),
    (1, 123),
    (2, 2314),
    (3, 9),
    (4, 1292),
    (5, 12831),
    (6, 12381),
    (7, 123999);

DELETE FROM
    warehouse.example_schema.deletion_vectors
WHERE
    c1 % 3 = 0;

INSERT INTO
    warehouse.example_schema.deletion_vectors
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
    warehouse.example_schema.deletion_vectors
WHERE
    c1 % 3 = 1;
