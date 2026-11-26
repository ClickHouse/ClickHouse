Data generated using Databricks 16.4:

```sql
CREATE TABLE test_checkpoint_on_schema_change (x int)
USING delta 
LOCATION ?
TBLPROPERTIES('delta.checkpointInterval' = 2);

INSERT INTO test_checkpoint_on_schema_change VALUES 1;
INSERT INTO test_checkpoint_on_schema_change VALUES 2;
CREATE OR REPLACE TABLE test_checkpoint_on_schema_change (x int, y string) 
TBLPROPERTIES('delta.checkpointInterval' = 20);
INSERT INTO test_checkpoint_on_schema_change values (4, 'version4');

CREATE OR REPLACE TABLE test_checkpoint_on_schema_change (y string) 
TBLPROPERTIES('delta.checkpointInterval' = 20);
INSERT INTO test_checkpoint_on_schema_change values 'version6';

CREATE OR REPLACE TABLE test_checkpoint_on_schema_change (z string) 
TBLPROPERTIES('delta.checkpointInterval' = 20);
INSERT INTO test_checkpoint_on_schema_change values 'version8';

CREATE OR REPLACE TABLE test_checkpoint_on_schema_change (z int) 
TBLPROPERTIES('delta.checkpointInterval' = 20);
INSERT INTO table test_checkpoint_on_schema_change values 10;

CREATE OR REPLACE TABLE test_checkpoint_on_schema_change (z string) 
TBLPROPERTIES('delta.checkpointInterval' = 20);
INSERT INTO test_checkpoint_on_schema_change values 'version12';
```

Note the 'delta.checkpointInterval' table property, set when creating or replacing the table,
is for removing distraction and ensuring checkpoints are created only when intended.
