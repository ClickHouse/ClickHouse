SET in_null_as_unknown = 1;

SELECT a IN materialize(b), a NOT IN materialize(b)
FROM values('a Array(UInt8), b Array(UInt8)', ([1], [1]), ([1], [2]));

DROP TABLE IF EXISTS squality_complex_array_rhs_in;
CREATE TABLE squality_complex_array_rhs_in
(
    a Array(UInt8),
    b Array(UInt8),
    CONSTRAINT c CHECK a IN materialize(b)
) ENGINE = Memory;
INSERT INTO squality_complex_array_rhs_in VALUES ([1], [1]);
INSERT INTO squality_complex_array_rhs_in VALUES ([1], [2]); -- { serverError VIOLATED_CONSTRAINT }
SELECT a IN materialize(b) FROM squality_complex_array_rhs_in;
DROP TABLE squality_complex_array_rhs_in;

DROP TABLE IF EXISTS squality_complex_array_rhs_not_in;
CREATE TABLE squality_complex_array_rhs_not_in
(
    a Array(UInt8),
    b Array(UInt8),
    CONSTRAINT c CHECK a NOT IN materialize(b)
) ENGINE = Memory;
INSERT INTO squality_complex_array_rhs_not_in VALUES ([1], [2]);
INSERT INTO squality_complex_array_rhs_not_in VALUES ([1], [1]); -- { serverError VIOLATED_CONSTRAINT }
SELECT a NOT IN materialize(b) FROM squality_complex_array_rhs_not_in;
DROP TABLE squality_complex_array_rhs_not_in;
