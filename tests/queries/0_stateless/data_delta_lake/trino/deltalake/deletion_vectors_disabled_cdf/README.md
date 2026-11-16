Data generated using Spark 4.1.0 and Delta Lake 4.4.0:

```sql
CREATE TABLE deletion_vectors_disabled_cdf (id INT, part INT)
USING delta
PARTITIONED BY (part)
LOCATION 'file:///data/deletion_vectors_disabled_cdf'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.enableChangeDataFeed' = 'true');
INSERT INTO deletion_vectors_disabled_cdf VALUES (1, 10), (2, 10), (3, 10), (4, 10), (5, 20), (6, 20);
DELETE FROM deletion_vectors_disabled_cdf WHERE id IN (2, 5);
ALTER TABLE deletion_vectors_disabled_cdf SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false');
```
