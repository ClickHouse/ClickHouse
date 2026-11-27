Data generated using Databricks 13.1:

```sql
CREATE TABLE default.test_timestamp_ntz 
(x timestamp_ntz) 
USING delta 
LOCATION 's3://bucket/table'
TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported');

INSERT INTO default.test_timestamp_ntz VALUES 
(NULL),
(TIMESTAMP_NTZ '-9999-12-31T23:59:59.999999'),
(TIMESTAMP_NTZ '-0001-01-01T00:00:00.000000'),
(TIMESTAMP_NTZ '0000-01-01T00:00:00.000000'), 
(TIMESTAMP_NTZ '1582-10-05T00:00:00.000000'), 
(TIMESTAMP_NTZ '1582-10-14T23:59:59.999999'), 
(TIMESTAMP_NTZ '2020-12-31T01:02:03.123456'), 
(TIMESTAMP_NTZ '9999-12-31T23:59:59.999999');
```
