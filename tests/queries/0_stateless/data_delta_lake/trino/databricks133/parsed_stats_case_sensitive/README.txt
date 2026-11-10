Data generated using Databricks 13.3 LTS

```
CREATE TABLE default.parsed_stats_case_sensitive (part INT, a_NuMbEr INT, a_StRiNg STRING)
USING DELTA
PARTITIONED BY (part)
LOCATION 's3://.../parsed_stats_case_sensitive'
TBLPROPERTIES (delta.checkpointInterval = 2);

insert into default.parsed_stats_case_sensitive  VALUES (100, 1,'ala'), (200, 2, 'kota');
insert into default.parsed_stats_case_sensitive VALUES (300, 3, 'osla');
insert into default.parsed_stats_case_sensitive VALUES (400, 4, 'zulu');
```
