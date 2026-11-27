Data generated using Delta Lake 3.0.0:

```sql
CREATE TABLE test_v2_checkpoint_json
(a INT, b INT)
USING delta 
LOCATION ?
TBLPROPERTIES ('delta.checkpointPolicy' = 'v2', 'delta.checkpointInterval' = '1');

INSERT INTO test_v2_checkpoint_json VALUES (1, 2);
```
