Data generated using Databricks 15.3:

```sql
CREATE TABLE default.test_type_widening_nested
(s struct<field: byte>, sn struct<field: byte, field2: struct<inner_field: byte>>, m map<byte, byte>, a array<byte>)
    USING DELTA
    LOCATION 's3://starburstdata-test/test-test-db-widening-nested'
    TBLPROPERTIES ('delta.enableTypeWidening'=true);
INSERT INTO default.test_type_widening_nested VALUES (named_struct('field',127), named_struct('field',127,'field2',named_struct('inner_field',15)), map(-128,127), array(127));
ALTER TABLE default.test_type_widening_nested CHANGE COLUMN s.field TYPE short;
ALTER TABLE default.test_type_widening_nested CHANGE COLUMN sn.field TYPE short;
ALTER TABLE default.test_type_widening_nested CHANGE COLUMN sn.field2.inner_field TYPE short;
ALTER TABLE default.test_type_widening_nested CHANGE COLUMN m.key TYPE short;
ALTER TABLE default.test_type_widening_nested CHANGE COLUMN m.value TYPE short;
ALTER TABLE default.test_type_widening_nested CHANGE COLUMN a.element TYPE short;
INSERT INTO default.test_type_widening_nested VALUES (named_struct('field',32767), named_struct('field',32767,'field2',named_struct('inner_field',32767)), map(-32768,32767), array(32767));
ALTER TABLE default.test_type_widening_nested CHANGE COLUMN s.field TYPE double;
-- one of two sn fields change to unsupported type
ALTER TABLE default.test_type_widening_nested CHANGE COLUMN sn.field2.inner_field TYPE double;
ALTER TABLE default.test_type_widening_nested CHANGE COLUMN m.key TYPE integer;
-- only map value change to unsupported type
ALTER TABLE default.test_type_widening_nested CHANGE COLUMN m.value TYPE double;
ALTER TABLE default.test_type_widening_nested CHANGE COLUMN a.element TYPE double;
INSERT INTO default.test_type_widening_nested VALUES (named_struct('field',-5555555555555555.1), named_struct('field',32757,'field2',named_struct('inner_field',-5555555555555555.1)), map(200,-5555555555555555.1), array(-5555555555555555.1));
```
