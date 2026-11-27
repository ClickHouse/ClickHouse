Data generated using Databricks 7.3 :

```
CREATE TABLE manual_testing.insert_nested_nonlowercase_columns(an_int INT, nested STRUCT<lower_case_string: STRING, UPPER_CASE_STRING: STRING, MiXeD_CaSe_StRiNg: STRING>)
USING DELTA
LOCATION 's3://starburst-alex/delta/manual_testing/insert_nested_nonlowercase_columns';

INSERT INTO manual_testing.insert_nested_nonlowercase_columns VALUES (1, struct('databricks', 'DATABRICKS', 'DaTaBrIcKs')), (2, struct('databricks', 'DATABRICKS', NULL));
INSERT INTO manual_testing.insert_nested_nonlowercase_columns VALUES (3, struct(NULL, NULL, 'DaTaBrIcKs')), (4, struct(NULL, NULL, NULL));
```
