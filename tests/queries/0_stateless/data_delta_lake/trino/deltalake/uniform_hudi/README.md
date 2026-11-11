Data generated using OSS Delta Lake 4.0.0-preview1:

```sql
CREATE TABLE default.test_uniform_hudi
(col1 int)
USING DELTA 
TBLPROPERTIES (
 'delta.universalFormat.enabledFormats' = 'hudi'
);

INSERT INTO default.test_uniform_hudi VALUES (123);
```
