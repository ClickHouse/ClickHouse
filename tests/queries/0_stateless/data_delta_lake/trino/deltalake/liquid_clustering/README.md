Data generated using OSS Delta Lake 3.2.0.
At least two columns are required for Hilbert clustering.

```sql
CREATE TABLE test_liquid 
(data string, year int, month int) 
USING delta 
CLUSTER BY (year, month)
LOCATION ?;

INSERT INTO test_liquid VALUES ('test 1', 2024, 1);

INSERT INTO test_liquid VALUES ('test 2', 2024, 2);

OPTIMIZE test_liquid;
```
