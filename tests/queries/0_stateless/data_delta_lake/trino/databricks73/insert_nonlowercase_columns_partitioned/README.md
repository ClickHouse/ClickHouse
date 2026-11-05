Data generated using Databricks 7.3 :

```
CREATE TABLE manual_testing.insert_nonlowercase_columns_partitioned(lower_case_string STRING, UPPER_CASE_STRING STRING, MiXeD_CaSe_StRiNg STRING)
USING delta
PARTITIONED BY (MiXeD_CaSe_StRiNg)
LOCATION 's3://starburst-alex/delta/manual_testing/insert_nonlowercase_columns_partitioned';

INSERT INTO manual_testing.insert_nonlowercase_columns_partitioned VALUES ('databricks', 'DATABRICKS', 'DaTaBrIcKs'), ('databricks', 'DATABRICKS', NULL);
INSERT INTO manual_testing.insert_nonlowercase_columns_partitioned VALUES (NULL, NULL, 'DaTaBrIcKs'), (NULL, NULL, NULL);
```
