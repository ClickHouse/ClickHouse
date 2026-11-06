Data generated using OSS Delta Lake 3.0.0:

```sql
CREATE TABLE test_partition_values_parsed (
 id INT,
 int_part INT,
 string_part STRING
)
USING delta 
PARTITIONED BY (int_part, string_part)
LOCATION 's3://test-bucket/test_partition_values_parsed'
TBLPROPERTIES (
 delta.checkpoint.writeStatsAsStruct = true,
 delta.checkpoint.writeStatsAsJson = false,
 delta.checkpointInterval = 1
);

INSERT INTO test_partition_values_parsed VALUES (1, 10, 'part1');
INSERT INTO test_partition_values_parsed VALUES (2, 20, 'part2');
INSERT INTO test_partition_values_parsed VALUES (3, NULL, NULL);
```
