-- Engine: Trino 450

CREATE SCHEMA IF NOT EXISTS warehouse.example_schema WITH (location = 's3a://warehouse/example');

CREATE TABLE warehouse.example_schema.year_date_partitioning (c1 INTEGER, c2 DATE, c3 DOUBLE) WITH (
    format = 'PARQUET',
    partitioning = ARRAY ['year(c2)'],
    location = 's3a://warehouse/year_date_partitioning'
);

INSERT INTO
    warehouse.example_schema.year_date_partitioning
VALUES
    (1, date '2025-03-03', double '0.0'),
    (2, date '2024-03-03', double '0.0'),
    (3, date '2025-03-03', double '0.0');
