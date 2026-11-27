Data generated using OSS Delta Lake 3.2.0:

```sql
CREATE TABLE default.append_only
(a int, b int)
USING delta
LOCATION ?
TBLPROPERTIES ('delta.appendOnly' = true);

INSERT INTO default.append_only VALUES (1, 11), (2, 12);
```
