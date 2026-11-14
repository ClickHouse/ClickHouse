Data generated using Trino 440:

```sql
CREATE TABLE time_travel WITH (checkpoint_interval = 2) AS SELECT 1 id;
INSERT INTO time_travel VALUES 2;
INSERT INTO time_travel VALUES 3;
INSERT INTO time_travel VALUES 4;
```
