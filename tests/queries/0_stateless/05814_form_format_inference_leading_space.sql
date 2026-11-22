-- Values of the Form format with leading or trailing whitespace (after decoding '+' or '%20')
-- must be inferred as strings, so that they can be read with the inferred schema.

DESCRIBE format(Form, 'a=+42');
SELECT * FROM format(Form, 'a=+42');
DESCRIBE format(Form, 'a=text&b=+42&c=%2042&d=42+&e=42');
SELECT * FROM format(Form, 'a=text&b=+42&c=%2042&d=42+&e=42');
