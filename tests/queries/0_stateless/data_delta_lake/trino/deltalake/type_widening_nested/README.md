Data generated using OSS Delta Lake 3.2.0:

```sql
CREATE TABLE default.test_type_widening_nested
(s struct<field: byte>, m map<byte, byte>, a array<byte>)
USING DELTA 
LOCATION 's3://test-bucket/databricks-compatibility-test-test-widening-nested'
TBLPROPERTIES ('delta.enableTypeWidening'=true);
INSERT INTO default.test_type_widening_nested VALUES (named_struct('field',127), map(-128,127), array(127));

ALTER TABLE default.test_type_widening_nested CHANGE COLUMN s.field TYPE short;
ALTER TABLE default.test_type_widening_nested CHANGE COLUMN m.key TYPE short;
ALTER TABLE default.test_type_widening_nested CHANGE COLUMN m.value TYPE short;
ALTER TABLE default.test_type_widening_nested CHANGE COLUMN a.element TYPE short;
INSERT INTO default.test_type_widening_nested VALUES (named_struct('field',32767), map(-32768,32767), array(32767));

ALTER TABLE default.test_type_widening_nested CHANGE COLUMN s.field TYPE integer;
ALTER TABLE default.test_type_widening_nested CHANGE COLUMN m.key TYPE integer;
ALTER TABLE default.test_type_widening_nested CHANGE COLUMN m.value TYPE integer;
ALTER TABLE default.test_type_widening_nested CHANGE COLUMN a.element TYPE integer;
INSERT INTO default.test_type_widening_nested VALUES (named_struct('field',2147483647), map(-2147483648,2147483647), array(2147483647));
```

Other type widening including from integer to long is not supported in 3.2.0.
