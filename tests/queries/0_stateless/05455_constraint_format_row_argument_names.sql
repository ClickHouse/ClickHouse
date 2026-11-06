-- A `CHECK` constraint is compiled as a standalone expression. `formatRow` writes the names of its
-- arguments to the header of the output format, so they must be kept when the expression is compiled.

DROP TABLE IF EXISTS constraint_format_row;

CREATE TABLE constraint_format_row
(
    x UInt8,
    y String,
    CONSTRAINT c CHECK formatRowNoNewline('JSONEachRow', x, y) = '{"x":1,"y":"a"}'
)
ENGINE = Memory;

INSERT INTO constraint_format_row VALUES (1, 'a');
INSERT INTO constraint_format_row VALUES (2, 'b'); -- { error VIOLATED_CONSTRAINT }

SELECT * FROM constraint_format_row;

DROP TABLE constraint_format_row;

-- An argument folded to a constant is named after the expression it was folded from.
CREATE TABLE constraint_format_row
(
    x UInt8,
    CONSTRAINT c CHECK JSONHas(formatRowNoNewline('JSONEachRow', x, 1 + 1), 'plus(1, 1)')
)
ENGINE = Memory;

INSERT INTO constraint_format_row VALUES (3);

SELECT * FROM constraint_format_row;

DROP TABLE constraint_format_row;
