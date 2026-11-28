SET in_null_as_unknown = 1;

SELECT map('a', '01') IN (map('a', toUInt8(1)));
SELECT map('a', '01') IN materialize(map('a', toUInt8(1)));
SELECT map('a', '01') IN (SELECT map('a', toUInt8(1))); -- { serverError NOT_IMPLEMENTED }
