Data generated using Databricks 15.3:

```sql
--      * byte	    -> short, int, long, decimal, double
--      * short	    -> int, long, decimal, double
--      * int	    -> long, decimal, double
--      * long	    -> decimal
--      * float	    -> double
--      * decimal	-> decimal with greater precision and scale
--      * date	    -> timestampNTZ
--
--  Covering all possible type changes
--  from https://docs.databricks.com/en/delta/type-widening.html#supported-type-changes

CREATE TABLE default.test_type_widening
    (
        byte_to_short BYTE, byte_to_int BYTE, byte_to_long BYTE, byte_to_decimal BYTE, byte_to_double BYTE,
        short_to_int SHORT, short_to_long SHORT, short_to_decimal SHORT, short_to_double SHORT,
        int_to_long INT, int_to_decimal INT, int_to_double INT,
        long_to_decimal LONG,
        float_to_double FLOAT,
        decimal_to_decimal DECIMAL,
        date_to_timestamp DATE
    )
    USING DELTA
    LOCATION 's3://starburstdata-test/test-db-type-widening'
    TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.feature.timestampNtz' = 'supported');

INSERT INTO default.test_type_widening VALUES
    (
        1, 1, 1, 1, 1,
        1, 1, 1, 1,
        1, 1, 1,
        1,
        1.0,
        1.0,
        DATE '2024-06-19'
    );

ALTER TABLE default.test_type_widening ALTER COLUMN byte_to_short TYPE SHORT;
ALTER TABLE default.test_type_widening ALTER COLUMN byte_to_int TYPE INT;
ALTER TABLE default.test_type_widening ALTER COLUMN byte_to_long TYPE LONG;
ALTER TABLE default.test_type_widening ALTER COLUMN byte_to_decimal TYPE DECIMAL;
ALTER TABLE default.test_type_widening ALTER COLUMN byte_to_double TYPE DOUBLE;
ALTER TABLE default.test_type_widening ALTER COLUMN short_to_int TYPE INT;
ALTER TABLE default.test_type_widening ALTER COLUMN short_to_long TYPE LONG;
ALTER TABLE default.test_type_widening ALTER COLUMN short_to_decimal TYPE DECIMAL;
ALTER TABLE default.test_type_widening ALTER COLUMN short_to_double TYPE DOUBLE;
ALTER TABLE default.test_type_widening ALTER COLUMN int_to_long TYPE LONG;
ALTER TABLE default.test_type_widening ALTER COLUMN int_to_decimal TYPE DECIMAL;
ALTER TABLE default.test_type_widening ALTER COLUMN int_to_double TYPE DOUBLE;
ALTER TABLE default.test_type_widening ALTER COLUMN long_to_decimal TYPE DECIMAL (20,0);
ALTER TABLE default.test_type_widening ALTER COLUMN float_to_double TYPE DOUBLE;
ALTER TABLE default.test_type_widening ALTER COLUMN decimal_to_decimal TYPE DECIMAL (12,2);
ALTER TABLE default.test_type_widening ALTER COLUMN date_to_timestamp TYPE TIMESTAMP_NTZ;

INSERT INTO default.test_type_widening VALUES
    (
        2, 2, 2, 2, 2,
        2, 2, 2, 2,
        2, 2, 2,
        2,
        2.0,
        2.22,
        TIMESTAMP_NTZ '2021-7-1T8:43:28.123456'
    );
```
