-- For a query in `body(...)` of the `url` table function, the `Content-Type` header is set from its output format,
-- unless it is specified in `headers(...)`. The request is sent to the HTTP interface of the server itself,
-- which executes the body as a query, and the query returns the header it received.

SET enable_analyzer = 1;

SELECT * FROM url('http://127.0.0.1:8123/?allow_get_client_http_header=1', TSV, 'x String',
    body(SELECT 'SELECT getClientHTTPHeader(\'Content-Type\') FORMAT TSV' FORMAT TSVRaw));

SELECT * FROM url('http://127.0.0.1:8123/?allow_get_client_http_header=1', TSV, 'x String',
    body(SELECT 'SELECT getClientHTTPHeader(\'Content-Type\') FORMAT TSV' FORMAT RawBLOB));

-- An explicit header is kept, whatever the case of its name.
SELECT * FROM url('http://127.0.0.1:8123/?allow_get_client_http_header=1', TSV, 'x String',
    headers('content-type' = 'text/plain'),
    body(SELECT 'SELECT getClientHTTPHeader(\'Content-Type\') FORMAT TSV' FORMAT TSVRaw));

-- A constant body has no format, so no header is added.
SELECT * FROM url('http://127.0.0.1:8123/?allow_get_client_http_header=1', TSV, 'x String',
    body('SELECT getClientHTTPHeader(\'Content-Type\') FORMAT TSV'));
