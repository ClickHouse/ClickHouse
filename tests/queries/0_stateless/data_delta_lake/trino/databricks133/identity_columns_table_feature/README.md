Data generated using Databricks 13.3:

```sql
CREATE TABLE default.identity_columns_table_feature
(a INT, b INT)
USING DELTA 
LOCATION ?
TBLPROPERTIES ('delta.feature.identityColumns' = 'supported');
```
