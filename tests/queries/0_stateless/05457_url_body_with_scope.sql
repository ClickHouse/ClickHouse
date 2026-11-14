-- The contents of `body(...)` of the `url` table function are resolved in the scope of the enclosing query,
-- so `WITH` aliases and CTEs can be used to build the request body.
-- The request is sent to the HTTP interface of the server itself, which executes the body as a query.

SET enable_analyzer = 1;

WITH 'SELECT 42 FORMAT TSV' AS payload
SELECT * FROM url('http://127.0.0.1:8123/', TSV, 'x UInt8', body(payload));

WITH 'SELECT ' || toString(40 + 3) || ' FORMAT TSV' AS payload
SELECT * FROM url('http://127.0.0.1:8123/', TSV, 'x UInt8', body(payload));

WITH 'SELECT 44 FORMAT TSV' AS payload
SELECT * FROM url('http://127.0.0.1:8123/', TSV, 'x UInt8', body((SELECT payload), TSVRaw));

WITH c AS (SELECT 'SELECT 45 FORMAT TSV' AS q)
SELECT * FROM url('http://127.0.0.1:8123/', TSV, 'x UInt8', body((SELECT q FROM c), TSVRaw));

-- A bare identifier is not turned into a string.
SELECT * FROM url('http://127.0.0.1:8123/', TSV, 'x UInt8', body(payload)); -- { serverError UNKNOWN_IDENTIFIER }

-- The body cannot depend on the rows of the enclosing query.
SELECT * FROM numbers(1) AS t, url('http://127.0.0.1:8123/', TSV, 'x UInt8', body(toString(t.number))); -- { serverError BAD_ARGUMENTS }
