-- Engine: Trino 450

CREATE SCHEMA IF NOT EXISTS warehouse.example_schema WITH (location = 's3a://warehouse/example');

CREATE TABLE warehouse.example_schema.year_timestamp_partitioning (c1 INTEGER, c2 TIMESTAMP(6), c3 DOUBLE) WITH (
    format = 'PARQUET',
    partitioning = ARRAY ['year(c2)'],
    location = 's3a://warehouse/year_timestamp_partitioning'
);

INSERT INTO
    warehouse.example_schema.year_timestamp_partitioning
VALUES
    (1, cast('2025-03-03 23:00:00' AS TIMESTAMP(6)), double '0.0'),
    (2, cast('2024-03-03 23:52:00' AS TIMESTAMP(6)), double '0.0'),
    (3, cast('2025-03-03 22:00:00' AS TIMESTAMP(6)), double '0.0');
