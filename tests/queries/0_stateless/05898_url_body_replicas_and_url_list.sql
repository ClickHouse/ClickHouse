-- A query in `body(...)` of the `url` table function is rejected when the URL generates several requests,
-- because the query would be executed for each of them. Whether it does is decided by the same expansion
-- the read uses: alternatives separated by `|` are failover options of a single request, also inside braces,
-- while a list separated by `,` (enabled by a `|` anywhere in the URL) is a separate request per element.
-- The requests are sent to the HTTP interface of the server itself, which executes the body as a query.

-- Braced failover alternatives: one request.
SELECT * FROM url('http://127.0.0.{1|1}:8123/', TSV, 'x UInt8',
    body(SELECT 'SELECT 42 FORMAT TSV' FORMAT TSVRaw));

-- Unbraced failover alternatives: one request.
SELECT * FROM url('http://127.0.0.1:8123/|http://127.0.0.1:8123/', TSV, 'x UInt8',
    body(SELECT 'SELECT 43 FORMAT TSV' FORMAT TSVRaw));

-- A list of two URLs, one of them with a failover alternative: two requests.
SELECT * FROM url('http://127.0.0.1:8123/,http://127.0.0.1:8123/|http://127.0.0.1:8123/', TSV, 'x UInt8',
    body(SELECT 'SELECT 44 FORMAT TSV' FORMAT TSVRaw)); -- { serverError BAD_ARGUMENTS }

-- A braced list: two requests.
SELECT * FROM url('http://127.0.0.{1,1}:8123/', TSV, 'x UInt8',
    body(SELECT 'SELECT 45 FORMAT TSV' FORMAT TSVRaw)); -- { serverError BAD_ARGUMENTS }

-- A constant body is sent with every request of a list.
SELECT * FROM url('http://127.0.0.1:8123/,http://127.0.0.1:8123/|http://127.0.0.1:8123/', TSV, 'x UInt8',
    body('SELECT 46 FORMAT TSV'));
