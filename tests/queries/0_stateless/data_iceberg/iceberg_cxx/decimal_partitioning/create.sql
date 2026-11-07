-- Engine: Trino 471
CREATE SCHEMA IF NOT EXISTS warehouse.example_schema WITH (location = 's3a://warehouse/example');

CREATE TABLE warehouse.example_schema.decimal_partitioning (
    col_decimal_9_2 DECIMAL(9, 2),
    col_decimal_18_2 DECIMAL(18, 2),
    col_decimal_19_2 DECIMAL(19, 2),
    col_decimal_38_2 DECIMAL(38, 2)
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY ['col_decimal_9_2', 'col_decimal_18_2', 'col_decimal_19_2', 'col_decimal_38_2'],
    location = 's3a://warehouse/decimal_partitioning'
);

INSERT INTO
    warehouse.example_schema.decimal_partitioning
VALUES
    (
        cast(3.0 AS DECIMAL(9, 2)),
        cast(-5.0 AS DECIMAL(18, 2)),
        cast(0.0 AS DECIMAL(19, 2)),
        cast(-0.0 AS DECIMAL(38, 2))
    );