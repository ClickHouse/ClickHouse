Data generated using Databricks 12.2:

```sql
CREATE TABLE default.identity_columns
(a INT, b BIGINT GENERATED ALWAYS AS IDENTITY)
USING DELTA
LOCATION 's3://trino-ci-test/identity_columns'
```
