-- Engine: Trino 471

CREATE SCHEMA IF NOT EXISTS warehouse.example_schema WITH (location = 's3a://warehouse/example');

CREATE TABLE warehouse.example_schema.bucket_partitioning (
    col_int INTEGER,
    col_long BIGINT,
    col_decimal DECIMAL(12, 2),
    col_date DATE,
    col_time TIME(6),
    col_timestamp TIMESTAMP(6),
    col_timestamptz TIMESTAMP(6) WITH TIME ZONE,
    col_string VARCHAR,
    col_uuid UUID,
    col_varbinary VARBINARY
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY ['bucket(col_int, 2)', 'bucket(col_long, 3)', 'bucket(col_decimal, 4)', 'bucket(col_date, 5)', 'bucket(col_time, 6)', 'bucket(col_timestamp, 100)', 'bucket(col_timestamptz, 123)', 'bucket(col_string, 42)', 'bucket(col_uuid, 55)', 'bucket(col_varbinary, 812)'],
    location = 's3a://warehouse/bucket_partitioning'
);

INSERT INTO
    warehouse.example_schema.bucket_partitioning
VALUES
    (
        1,
        cast(2 AS BIGINT),
        cast(3.0 AS DECIMAL(9, 2)),
        date '2025-03-03',
        cast('17:00:00.000000' AS TIME(6)),
        cast('2025-03-03 00:00:00' AS TIMESTAMP(6)),
        cast(
            '2025-03-03 00:00:00' AS TIMESTAMP(6) WITH TIME ZONE
        ),
        cast('some-string' AS VARCHAR),
        cast('12151fd2-7586-11e9-8f9e-2a86e4085a59' AS UUID),
        cast('some-varbinary' AS VARBINARY)
    );