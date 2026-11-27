Data generated using Trino:

```sql
CREATE TABLE no_stats_various_types (c_boolean boolean, c_tinyint tinyint, c_smallint smallint, c_integer integer, c_bigint bigint, c_real real, c_double double, c_decimal1 decimal(5,3), c_decimal2 decimal(15,3), c_date1 date, c_timestamp "timestamp with time zone", c_varchar1 varchar(3), c_varchar2 varchar, c_varbinary varbinary);
INSERT INTO no_stats_various_types VALUES (false, 37, 32123, 1274942432, 312739231274942432, REAL '567.123', DOUBLE '1234567890123.123', 12.345, 123456789012.345, DATE '1999-01-01', TIMESTAMP '2020-02-12 15:03:00', 'ab', 'de',  X'12ab3f');
INSERT INTO no_stats_various_types VALUES 
(true, 127, 32767, 2147483647, 9223372036854775807, REAL '999999.999', DOUBLE '9999999999999.999', 99.999, 999999999999.99, DATE '2028-10-04', TIMESTAMP '2199-12-31 23:59:59.999', 'zzz', 'zzz',  X'ffffffffffffffffffff'),                
(null,null,null,null,null,null,null,null,null,null,null,null,null,null);
INSERT INTO no_stats_various_types VALUES (null,null,null,null,null,null,null,null,null,null,null,null,null,null);
```

with removed:

- stats entries from json files
- json crc files
- _trino_metadata directory
