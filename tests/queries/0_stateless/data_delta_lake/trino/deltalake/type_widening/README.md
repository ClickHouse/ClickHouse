Data generated using OSS Delta Lake 3.2.0:

```sql
CREATE TABLE default.test_type_widening
(col byte)
USING DELTA 
LOCATION 's3://test-bucket/databricks-compatibility-test-test-widening'
TBLPROPERTIES ('delta.enableTypeWidening'=true);
INSERT INTO default.test_type_widening VALUES 127;

ALTER TABLE default.test_type_widening CHANGE COLUMN col TYPE short;
INSERT INTO default.test_type_widening VALUES 32767;

ALTER TABLE default.test_type_widening CHANGE COLUMN col TYPE integer;
INSERT INTO default.test_type_widening VALUES 2147483647;
```

Other type widening including from integer to long is not supported in 3.2.0.
