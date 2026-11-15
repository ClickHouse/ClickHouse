Data generated using Databricks 7.3 :

```
CREATE TABLE default.simple_table_without_checkpoint(a_column INT)
USING delta
LOCATION 's3://starburst-alex/delta/manual_testing/simple_table_without_checkpoint';

INSERT INTO default.simple_table_without_checkpoint VALUES(1);
INSERT INTO default.simple_table_without_checkpoint VALUES(2);
INSERT INTO default.simple_table_without_checkpoint VALUES(3);
INSERT INTO default.simple_table_without_checkpoint VALUES(4);
INSERT INTO default.simple_table_without_checkpoint VALUES(5);
INSERT INTO default.simple_table_without_checkpoint VALUES(6);
INSERT INTO default.simple_table_without_checkpoint VALUES(7);
INSERT INTO default.simple_table_without_checkpoint VALUES(8);
INSERT INTO default.simple_table_without_checkpoint VALUES(9);
```
