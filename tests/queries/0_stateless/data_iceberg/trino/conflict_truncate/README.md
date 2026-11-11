The table is created in a way that produces a corrupted metadata file by applying multiple truncate levels on the same column.
It is created in Trino `476` using `IcebergFlociHiveMetastoreQueryRunnerMain`, with an initial `truncate(a, 1)` partitioning,
followed by an update to `truncate(a, 10)` on the same column, which introduces the conflicting partition transforms.

Here is the relevant partition spec of the corrupted metadata file. `a_trunc` is created twice with different truncate widths and field ids.
This leads to conflicts when trying to resolve the partitioning for the column `a` while querying `SELECT partition FROM "my_table$files"`

```json
    "partition-specs" : [
        {
            "spec-id": 0,
            "fields": [
                {
                    "name": "a_trunc",
                    "transform": "truncate[1]",
                    "source-id": 1,
                    "field-id": 1000
                }
            ]
        },
        {
            "spec-id": 1,
            "fields": [
                {
                    "name": "a_trunc",
                    "transform": "truncate[10]",
                    "source-id": 1,
                    "field-id": 1001
                }
            ]
        }
    ]
```

Use `trino` to create the table content:

```sql
-- Create a table that uses truncate(a, 1) as the initial partition transform
CREATE TABLE conflict_truncate (a varchar) WITH (location ='s3://test-partition-evolution/conflict_truncate', partitioning = ARRAY['truncate(a, 1)']);
INSERT INTO conflict_truncate VALUES 'abc';

-- Update the table's partitioning to use a different truncate width (10). This introduces conflicting truncate levels on the same column
ALTER TABLE conflict_truncate SET PROPERTIES partitioning = ARRAY['truncate(a, 10)'];
INSERT INTO conflict_truncate VALUES 'abcd';
```
