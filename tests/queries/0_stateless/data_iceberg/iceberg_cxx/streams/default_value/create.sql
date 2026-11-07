CREATE SCHEMA IF NOT EXISTS streams;

CREATE TABLE streams.default_value (
    id BIGINT
)
USING iceberg;


INSERT INTO streams.default_value (id) VALUES (1);

ALTER TABLE streams.default_value ADD COLUMN boolean_col BOOLEAN;
ALTER TABLE streams.default_value ADD COLUMN integer_col INT;
ALTER TABLE streams.default_value ADD COLUMN long_col BIGINT;
ALTER TABLE streams.default_value ADD COLUMN float_col FLOAT;
ALTER TABLE streams.default_value ADD COLUMN double_col DOUBLE;
ALTER TABLE streams.default_value ADD COLUMN decimal_col DECIMAL(10, 2);
ALTER TABLE streams.default_value ADD COLUMN string_col STRING;
ALTER TABLE streams.default_value ADD COLUMN binary_col BINARY;
ALTER TABLE streams.default_value ADD COLUMN date_col DATE;
ALTER TABLE streams.default_value ADD COLUMN timestamp_col TIMESTAMP;
ALTER TABLE streams.default_value ADD COLUMN timestamptz_col TIMESTAMP;
ALTER TABLE streams.default_value ADD COLUMN array_col ARRAY<STRING>;


INSERT INTO streams.default_value (
    id,
    boolean_col,
    integer_col,
    long_col,
    float_col,
    double_col,
    decimal_col,
    string_col,
    binary_col,
    date_col,
    timestamp_col,
    timestamptz_col,
    array_col
) VALUES (
        2,
        TRUE,
        2147483647,
        9223372036854775807,
        3.4028235E38,
        1.7976931348623157E308,
        12345.67,
        'example',
        X'737061726b',
        DATE '2023-10-10',
        TIMESTAMP '2023-10-10 12:34:56',
        TIMESTAMP '2023-10-10 12:34:56',
        ARRAY('item1', 'item2')
);
