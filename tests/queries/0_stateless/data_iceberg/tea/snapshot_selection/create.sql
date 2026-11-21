-- Engine: Trino 481
CREATE SCHEMA IF NOT EXISTS warehouse.example_schema WITH (location = 's3a://warehouse/example');

CREATE TABLE warehouse.example_schema.snapshot_selection (c1 INTEGER, c2 INTEGER) WITH (
    format = 'PARQUET',
    location = 's3a://warehouse/snapshot_selection'
);

INSERT INTO
    warehouse.example_schema.snapshot_selection
VALUES
    (0, 12),
    (1, 123),
    (2, 2314),
    (3, 9),
    (4, 1292),
    (5, 12831),
    (6, 12381),
    (7, 123999);

SELECT * FROM warehouse.example_schema."snapshot_selection$files";
