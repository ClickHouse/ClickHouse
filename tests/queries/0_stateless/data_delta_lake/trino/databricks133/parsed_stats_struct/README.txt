Data generated using Databricks 13.3 LTS

```
CREATE TABLE default.parsed_stats_struct (part INT, id INT, root STRUCT<entry_one : INT, entry_two : STRING>)
USING DELTA
PARTITIONED BY (part)
LOCATION 's3://?/parsed_stats_struct'
TBLPROPERTIES (delta.checkpointInterval = 2);


INSERT INTO default.parsed_stats_struct  VALUES (100, 1, STRUCT(1,'ala')), (200, 2, STRUCT(2, 'kota'));
INSERT INTO default.parsed_stats_struct VALUES (300, 3, STRUCT(3, 'osla'));
INSERT INTO default.parsed_stats_struct VALUES (400, 4, STRUCT(4, 'zulu'));
```
