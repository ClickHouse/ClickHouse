SET in_null_as_unknown = 1;

SELECT '01' IN (1), '01' IN materialize(toUInt8(1));
SELECT '01' IN (SELECT toUInt8(1)); -- { serverError NOT_IMPLEMENTED }

SELECT toUUID('00000000-0000-0000-0000-0000000000ab') IN
    materialize('00000000-0000-0000-0000-0000000000AB');
SELECT toUUID('00000000-0000-0000-0000-0000000000ab') IN
    (SELECT '00000000-0000-0000-0000-0000000000AB'); -- { serverError NOT_IMPLEMENTED }

DROP TABLE IF EXISTS squality_prepared_mixed_string_set;
CREATE TABLE squality_prepared_mixed_string_set (k UInt8) ENGINE = Set;
INSERT INTO squality_prepared_mixed_string_set VALUES (1);
SELECT '01' IN squality_prepared_mixed_string_set; -- { serverError NOT_IMPLEMENTED }
DROP TABLE squality_prepared_mixed_string_set;
