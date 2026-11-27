-- Tags: no-fasttest

-- Schema inference for the Form format infers each field from its own value.
-- A field's type must not depend on the fields before it. JSONEachRow shows the
-- inferred type of each value: a number is unquoted, a String is quoted.

-- No String field before it.
SELECT * FROM format(Form, 'n=1') FORMAT JSONEachRow;

-- A String field first must not turn the next field into a String.
SELECT * FROM format(Form, 'a=x&n=1') FORMAT JSONEachRow;

-- The same fields in the other order infer the same types.
SELECT * FROM format(Form, 'n=1&a=x') FORMAT JSONEachRow;

-- Neither of two Int fields is affected.
SELECT * FROM format(Form, 'n=1&m=2') FORMAT JSONEachRow;

-- A String field in the middle affects only itself.
SELECT * FROM format(Form, 'n=1&a=x&m=2') FORMAT JSONEachRow;

-- A Float field keeps its own type after a String field.
SELECT * FROM format(Form, 'n=1&a=x&f=1.5') FORMAT JSONEachRow;

-- The decoded value is what gets inferred: %2B keeps the field a String.
SELECT * FROM format(Form, 'n=1&b=1%2B2') FORMAT JSONEachRow;
