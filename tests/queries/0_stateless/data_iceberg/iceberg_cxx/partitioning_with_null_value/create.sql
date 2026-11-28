-- Engine: Trino 471

CREATE SCHEMA IF NOT EXISTS warehouse.example_schema WITH (location = 's3a://warehouse/example');

CREATE TABLE warehouse.example_schema.partitioning_with_null_value (c1 INTEGER, c2 DATE, c3 DOUBLE) WITH (
    format = 'PARQUET',
    partitioning = ARRAY ['c1', 'c2'],
    location = 's3a://warehouse/partitioning_with_null_value'
);

INSERT INTO
    warehouse.example_schema.partitioning_with_null_value
VALUES
    (2, date '2025-03-03', double '-1.0'),
    (null, date '2025-03-03', double '0.0'),
    (1, null, double '-50'),
    (null, null, double '17');
