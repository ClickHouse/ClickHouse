-- Engine: Trino 408

CREATE SCHEMA IF NOT EXISTS warehouse.example_schema WITH (location = 's3a://warehouse/example');

CREATE TABLE warehouse.example_schema.no_partitioning (c1 INTEGER, c2 DATE, c3 DOUBLE) WITH (
    format = 'PARQUET',
    location = 's3a://warehouse/no_partitioning'
);

INSERT INTO
    warehouse.example_schema.no_partitioning
VALUES
    (1, date '2025-03-03', double '0.0'),
    (2, date '2025-03-03', double '-1.0'),
    (1, date '2025-03-04', double '1.5'),
    (2, date '2025-03-04', double '42'),
    (1, date '2025-03-05', double '17'),
    (1, date '2025-03-02', double '-50');
