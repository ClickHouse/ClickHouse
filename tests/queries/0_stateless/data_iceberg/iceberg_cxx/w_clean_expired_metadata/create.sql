-- Engine: Trino 480

CREATE SCHEMA IF NOT EXISTS warehouse.example_schema WITH (location = 's3a://warehouse/example');

CREATE TABLE warehouse.example_schema.w_clean_expired_metadata (c1 INTEGER, c2 TIMESTAMP(6) WITH TIME ZONE, c3 DOUBLE) WITH (
    format = 'PARQUET',
    location = 's3a://warehouse/w_clean_expired_metadata'
);

INSERT INTO
    warehouse.example_schema.w_clean_expired_metadata
VALUES
    (1, cast('2025-03-03 23:00:00' AS TIMESTAMP(6) WITH TIME ZONE), double '0.0'),
    (2, cast('2024-03-03 23:52:00' AS TIMESTAMP(6) WITH TIME ZONE), double '0.0'),
    (3, null, double '0.0');

ALTER TABLE warehouse.example_schema.w_clean_expired_metadata SET PROPERTIES partitioning = ARRAY ['day(c2)'];

DELETE FROM warehouse.example_schema.w_clean_expired_metadata;

INSERT INTO
    warehouse.example_schema.w_clean_expired_metadata
VALUES
    (1, cast('2025-03-03 23:00:00' AS TIMESTAMP(6) WITH TIME ZONE), double '0.0'),
    (2, cast('2024-03-03 23:52:00' AS TIMESTAMP(6) WITH TIME ZONE), double '0.0'),
    (3, null, double '0.0');

SET SESSION warehouse.expire_snapshots_min_retention = '1s';

ALTER TABLE warehouse.example_schema.w_clean_expired_metadata EXECUTE expire_snapshots(retention_threshold => '1s', clean_expired_metadata => true);
