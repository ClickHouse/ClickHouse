-- Values of the Form format in single quotes must be inferred as strings, because the type
-- inference looks inside the quotes, while the value is read verbatim with the quotes.

DESCRIBE format(Form, 'a=text&b=%272026-11-17%27&c=%272026-11-17+10%3A20%3A30%27');
SELECT * FROM format(Form, 'a=text&b=%272026-11-17%27&c=%272026-11-17+10%3A20%3A30%27');
DESCRIBE format(Form, 'a=%272026-11-17%27');
SELECT * FROM format(Form, 'a=%272026-11-17%27');
-- Unquoted dates are still inferred as dates.
DESCRIBE format(Form, 'a=text&b=2026-11-17&c=2026-11-17+10%3A20%3A30');
SELECT * FROM format(Form, 'a=text&b=2026-11-17&c=2026-11-17+10%3A20%3A30');
