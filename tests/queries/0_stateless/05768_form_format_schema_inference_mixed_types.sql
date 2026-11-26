-- Schema inference for the Form format must infer the type of each field from its own value only.

DESCRIBE format(Form, 'a=hello+world&b=42&c=3.5&d=%2B1');
SELECT b + 1 FROM format(Form, 'a=hello+world&b=42');
SELECT * FROM format(Form, 'a=hello+world&b=42&c=3.5&d=%2B1');
