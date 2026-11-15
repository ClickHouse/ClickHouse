Data generated using Trino:

```sql
CREATE TABLE no_stats (c_int, c_str) AS VALUES (42, 'foo'), (12, 'ab'), (null, null);
INSERT INTO no_stats VALUES (15,'cd'), (15,'bar');
```

with removed:

- stats entries from json files
- json crc files
- _trino_metadata directory
