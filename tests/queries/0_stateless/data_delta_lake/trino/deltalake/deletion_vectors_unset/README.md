Data generated using Spark 4.1.0 and Delta Lake 4.4.0:

```sql
CREATE TABLE deletion_vectors_unset (id INT, part INT)
USING delta
PARTITIONED BY (part)
LOCATION 'file:///data/deletion_vectors_unset'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');
INSERT INTO deletion_vectors_unset VALUES (1, 10), (2, 10), (3, 10), (4, 10), (5, 20), (6, 20);
DELETE FROM deletion_vectors_unset WHERE id IN (2, 5);
ALTER TABLE deletion_vectors_unset UNSET TBLPROPERTIES ('delta.enableDeletionVectors');
```
