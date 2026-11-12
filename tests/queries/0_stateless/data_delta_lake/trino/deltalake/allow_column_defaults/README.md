Data generated using OSS Delta Lake 3.2.0:

```sql
CREATE TABLE test_allow_column_defaults
(a int, b int default 16)
USING delta
LOCATION ?
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'enabled'
);

INSERT INTO test_allow_column_defaults (a) VALUES (1);
```
