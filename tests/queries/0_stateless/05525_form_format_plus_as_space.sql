-- The Form format reads application/x-www-form-urlencoded data, where '+' encodes a space.
-- '%2B' must still decode to a literal '+', and '+' must be decoded regardless of a preceding '?'.

-- Explicit structure
SELECT * FROM format(Form, 'a String, b UInt8', 'b=5&a=hello+world');
SELECT * FROM format(Form, 'a String, b String, c String', 'a=1%2B2&b=a%20b+c&c=x?y+z');

-- Schema inference
SELECT * FROM format(Form, 'a=hello+world%21&b=1%2B2&c=a%20b');
