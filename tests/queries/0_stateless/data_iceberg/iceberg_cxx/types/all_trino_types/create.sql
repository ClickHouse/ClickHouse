-- Engine: Trino 471

CREATE SCHEMA IF NOT EXISTS warehouse.types
WITH (location = 's3a://warehouse/');

CREATE TABLE warehouse.types.all_trino_types (
    id BIGINT,
    boolean_col BOOLEAN,
    integer_col INTEGER,
    long_col BIGINT,
    float_col REAL,
    double_col DOUBLE,
    decimal_col DECIMAL(18, 5),
    date_col DATE,
    time_col TIME(6),
    timestamp_col TIMESTAMP(6),
    timestamptz_col TIMESTAMP(6) WITH TIME ZONE,
    string_col VARCHAR,
    uuid_col UUID,
    binary_col VARBINARY,
    list_col ARRAY<DOUBLE>
)
WITH (
    format = 'PARQUET',
    location = 's3a://warehouse/types/all_trino_types/'
); 

INSERT INTO warehouse.types.all_trino_types VALUES (
  1,
  true,
  42,
  9223372036854775807,
  REAL '3.14',
  DOUBLE '2.718281828459045',
  DECIMAL '123456.78901',
  DATE '2025-04-05',
  TIME '12:34:56.123456',
  TIMESTAMP '2025-04-05 12:34:56.123456',
  TIMESTAMP '2025-04-05 12:34:56.123456 UTC',
  'Hello Iceberg',
  UUID '550e8400-e29b-41d4-a716-446655440000',
  from_hex('DEADBEEF'),
  ARRAY[DOUBLE '1.1', DOUBLE '2.2', DOUBLE '3.3']
);
