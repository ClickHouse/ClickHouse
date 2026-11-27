Data generated using Spark 4.1.0 and Delta Lake 4.4.0:

```sql
CREATE TABLE deletion_vectors_disabled_checkpoint (id INT, part INT)
USING delta
PARTITIONED BY (part)
LOCATION 'file:///data/deletion_vectors_disabled_checkpoint'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.checkpointInterval' = '4');
INSERT INTO deletion_vectors_disabled_checkpoint VALUES (1, 10), (2, 10), (3, 10), (4, 10), (5, 20), (6, 20);
DELETE FROM deletion_vectors_disabled_checkpoint WHERE id IN (2, 5);
ALTER TABLE deletion_vectors_disabled_checkpoint SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false');
-- Spark writes the checkpoint for version 4 holding add entries with deletion vectors
INSERT INTO deletion_vectors_disabled_checkpoint VALUES (7, 20);
```
