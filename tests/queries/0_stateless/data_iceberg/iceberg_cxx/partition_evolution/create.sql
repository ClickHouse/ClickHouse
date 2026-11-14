-- Engine: Trino 408
CREATE SCHEMA IF NOT EXISTS warehouse.example_schema WITH (location = 's3a://warehouse/example');

CREATE TABLE warehouse.example_schema.partition_evolution (c1 INTEGER, c2 DATE, c3 DOUBLE) WITH (
    format = 'PARQUET',
    partitioning = ARRAY ['c1', 'c2'],
    location = 's3a://warehouse/partition_evolution'
);

INSERT INTO
    warehouse.example_schema.partition_evolution
VALUES
    (1, date '2025-03-03', double '0.0'),
    (2, date '2025-03-03', double '-1.0'),
    (1, date '2025-03-04', double '1.5'),
    (2, date '2025-03-04', double '42'),
    (1, date '2025-03-05', double '17'),
    (1, date '2025-03-02', double '-50');

ALTER TABLE
    warehouse.example_schema.partition_evolution
SET
    PROPERTIES partitioning = ARRAY ['c3'];

INSERT INTO
    warehouse.example_schema.partition_evolution
VALUES
    (3, date '2025-03-11', double '2.0');
