-- The type inferred for a value of the Form format must be readable by the reader of that type,
-- which reads the value verbatim. A nested date with an escape sequence is unescaped by the type
-- inference, but not by the reader of `Date`, so it is kept as a string.
DESCRIBE format(Form, 'a=text&b=%5B%272026%5Cx2D11%5Cx2D17%27%5D');
SELECT * FROM format(Form, 'a=text&b=%5B%272026%5Cx2D11%5Cx2D17%27%5D');
-- A nested date without escape sequences is still inferred as a date.
DESCRIBE format(Form, 'a=text&b=%5B%272026-11-17%27%5D');
SELECT * FROM format(Form, 'a=text&b=%5B%272026-11-17%27%5D');

-- A one-element tuple with a trailing comma is inferred as a one-element tuple.
SELECT toTypeName(b), b FROM format(Form, 'a=text&b=%281%2C%29');
SELECT toTypeName(b), b FROM format(Form, 'a=text&b=%281%2C2%29');
SELECT toTypeName(c1), c1 FROM format(Values, '((1,))');
