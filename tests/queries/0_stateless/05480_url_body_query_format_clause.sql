-- The query in `body(...)` of the `url` table function can specify the output format in its `FORMAT` clause.
-- The request is sent to the HTTP interface of the server itself, which executes the body as a query.

SET enable_analyzer = 1;

-- A bare query, with and without the `FORMAT` clause.
SELECT * FROM url('http://127.0.0.1:8123/', TSV, 'x UInt8', body(SELECT 'SELECT 1 FORMAT TSV' FORMAT TSVRaw));
SELECT * FROM url('http://127.0.0.1:8123/', TSV, 'x UInt8', body(WITH 'SELECT 2 FORMAT TSV' AS q SELECT q FORMAT TSVRaw));
SELECT * FROM url('http://127.0.0.1:8123/', TSV, 'x UInt8', body(SELECT 'SELECT 3 FORMAT TSV' SETTINGS max_threads = 1 FORMAT TSVRaw));

-- The `FORMAT` clause is the same as the format argument after a parenthesized subquery.
SELECT * FROM url('http://127.0.0.1:8123/', TSV, 'x UInt8', body((SELECT 'SELECT 4 FORMAT TSV'), TSVRaw));
SELECT formatQuerySingleLine('SELECT * FROM url(\'http://127.0.0.1:8123/\', body(SELECT 1 FORMAT TSVRaw))');

-- The query can be in parentheses, also with the `FORMAT` clause.
SELECT * FROM url('http://127.0.0.1:8123/', TSV, 'x UInt8', body((SELECT 'SELECT 12 FORMAT TSV' FORMAT TSVRaw)));

-- `SETTINGS` after `FORMAT` are moved into the query, also to a `UNION`.
SELECT * FROM url('http://127.0.0.1:8123/', TSV, 'x UInt64',
    body(SELECT 'SELECT ' || getSetting('max_threads')::String || ' FORMAT TSV' FORMAT TSVRaw SETTINGS max_threads = 13));
SELECT * FROM url('http://127.0.0.1:8123/', TSV, 'x UInt64',
    body((SELECT 'SELECT ' || (getSetting('max_threads') + getSetting('max_block_size'))::String || ' FORMAT TSV' SETTINGS max_threads = 1 FORMAT TSVRaw SETTINGS max_block_size = 13)));
SELECT formatQuerySingleLine('SELECT * FROM url(\'http://127.0.0.1:8123/\', body((SELECT 1 UNION ALL SELECT 2 FORMAT TSVRaw SETTINGS max_threads = 1)))');

-- The query cannot be followed by other arguments, and `INTO OUTFILE` is not supported.
SELECT * FROM url('http://127.0.0.1:8123/', TSV, 'x UInt8', body(SELECT 1 FORMAT TSVRaw, TSV)); -- { clientError SYNTAX_ERROR }
SELECT * FROM url('http://127.0.0.1:8123/', TSV, 'x UInt8', body(SELECT 1 INTO OUTFILE 'body.tsv')); -- { clientError SYNTAX_ERROR }

-- Positional arguments are resolved once: `ORDER BY 1` refers to the constant `5`, which is not a position again.
SELECT * FROM url('http://127.0.0.1:8123/', TSV, 'x UInt8',
    body(SELECT q FROM (SELECT 5 AS k, 'SELECT 5 FORMAT TSV' AS q ORDER BY 1) FORMAT TSVRaw));
SELECT * FROM url('http://127.0.0.1:8123/', TSV, 'x UInt8',
    body(SELECT q FROM (SELECT 6 AS k, 'SELECT 6 FORMAT TSV' AS q GROUP BY 1, 2) FORMAT TSVRaw));

-- `MATERIALIZED` CTEs are not supported in the body.
SET enable_materialized_cte = 1;
SELECT * FROM url('http://127.0.0.1:8123/', TSV, 'x UInt8',
    body(WITH c AS MATERIALIZED (SELECT 'SELECT 7 FORMAT TSV' AS q) SELECT q FROM c FORMAT TSVRaw)); -- { serverError NOT_IMPLEMENTED }
WITH c AS MATERIALIZED (SELECT 'SELECT 8 FORMAT TSV' AS q)
SELECT * FROM url('http://127.0.0.1:8123/', TSV, 'x UInt8', body(SELECT q FROM c FORMAT TSVRaw)); -- { serverError NOT_IMPLEMENTED }

-- A pattern generating multiple URLs sends a request to each of them: a constant body is sent with every request,
-- while a query body is rejected, because it would be executed for every URL.
SELECT * FROM url('http://127.0.0.{1,2}:8123/', TSV, 'x UInt8', body('SELECT 9 FORMAT TSV'));
SELECT * FROM url('http://127.0.0.{1,2}:8123/', TSV, 'x UInt8', body(SELECT 'SELECT 10 FORMAT TSV' FORMAT TSVRaw)); -- { serverError BAD_ARGUMENTS }
-- Alternatives separated by `|` are failover options of a single request.
SELECT * FROM url('http://127.0.0.1:8123/|http://127.0.0.2:8123/', TSV, 'x UInt8', body(SELECT 'SELECT 11 FORMAT TSV' FORMAT TSVRaw));
