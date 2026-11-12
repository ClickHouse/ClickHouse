Data generated using Trino 432:

```sql
CREATE TABLE test_partition_values_parsed (
 id INT,
 int_part INT,
 string_part VARCHAR
)
WITH (
 partitioned_by = ARRAY['int_part', 'string_part'],
 checkpoint_interval = 1
);

INSERT INTO test_partition_values_parsed VALUES (1, 10, 'part1');
INSERT INTO test_partition_values_parsed VALUES (2, 20, 'part2');
INSERT INTO test_partition_values_parsed VALUES (3, NULL, NULL);
```
