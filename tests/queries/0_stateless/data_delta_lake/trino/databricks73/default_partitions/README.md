Data generated using Databricks 7.3 :

```
%sql
CREATE TABLE default.default_partitions(number_partition INT, string_partition STRING, value STRING)
   USING delta
   PARTITIONED BY (number_partition, string_partition)
   LOCATION 's3://starburst-alex/delta/manual_testing/default_partitions';

INSERT INTO default.default_partitions VALUES (NULL, 'partition_a', 'jarmuz');
INSERT INTO default.default_partitions VALUES (1, NULL, 'brukselka');
INSERT INTO default.default_partitions VALUES (NULL, NULL, 'kalafior');
```
