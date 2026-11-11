Data generated with Databricks 13.3 LTS:


```
CREATE TABLE partition_values_parsed_case_sensitive (id INT, part_NuMbEr INT, part_StRiNg STRING)
USING DELTA
PARTITIONED BY (part_NuMbEr, part_StRiNg)
LOCATION 's3://bucket/table'
TBLPROPERTIES (delta.checkpointInterval = 2);

INSERT INTO partition_values_parsed_case_sensitive VALUES (100, 1,'ala');
INSERT INTO partition_values_parsed_case_sensitive VALUES (200, 2,'kota');
INSERT INTO partition_values_parsed_case_sensitive VALUES (300, 3, 'osla');
```
