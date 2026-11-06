Data generated using Databricks 7.3 :

```
CREATE TABLE manual_testing.uppercase_columns(ALA BIGINT, KOTA BIGINT)
USING delta
PARTITIONED BY (ALA)
LOCATION 's3://starburst-alex/delta/manual_testing/uppercase_columns';

INSERT INTO manual_testing.uppercase_columns VALUES (1,1), (1,2), (2,1)
```
