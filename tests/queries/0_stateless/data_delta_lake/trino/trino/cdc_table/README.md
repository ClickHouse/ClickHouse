Data generated using Trino:

```sql
CREATE TABLE cdc_table WITH (partitioned_by=ARRAY['key'], change_data_feed_enabled=true) AS SELECT 1 key, 'a' value;
UPDATE cdc_table SET key = 2, value = 'a' WHERE key = 1
```
