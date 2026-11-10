Data generated using Delta Lake 3.2.0:

```sql
CREATE TABLE test_deletion_vector_pages
(id INT)
USING delta 
LOCATION 's3://test-bucket/test_deletion_vector_pages3'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.enableChangeDataFeed' = 'true');

-- Write 2 pages and update a row in the second page
-- 20000 is the default size of DEFAULT_PAGE_ROW_COUNT_LIMIT
INSERT INTO test_deletion_vector_pages SELECT explode(sequence(1, 20001));
UPDATE test_deletion_vector_pages SET id = 99999 WHERE id = 20001;
```
