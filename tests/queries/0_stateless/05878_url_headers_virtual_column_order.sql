-- The `url` table function added the `_headers` virtual column to its chunks after all the other
-- virtual columns, while the header of the source lists the virtual columns in the order they are
-- requested in. With `_headers` requested before another virtual column, the query read one column
-- in place of the other: an exception about the type of a column at best, a server crash otherwise.

-- The HTTP interface adds the header of the `http_response_headers` setting, `{'X-Test':'hello'}`, to its response.

SELECT '-- _headers before another virtual column';
SELECT _headers['X-Test'], _path, s
FROM url('http://127.0.0.1:8123/?query=SELECT+1&http_response_headers=%7B%27X-Test%27%3A%27hello%27%7D', LineAsString, 's String');

SELECT '-- _headers between other virtual columns';
SELECT _path, _headers['X-Test'], _file, s
FROM url('http://127.0.0.1:8123/?query=SELECT+1&http_response_headers=%7B%27X-Test%27%3A%27hello%27%7D', LineAsString, 's String');

SELECT '-- _headers after the other virtual columns';
SELECT _path, _file, _headers['X-Test'], s
FROM url('http://127.0.0.1:8123/?query=SELECT+1&http_response_headers=%7B%27X-Test%27%3A%27hello%27%7D', LineAsString, 's String');

SELECT '-- _headers in SELECT, another virtual column in WHERE';
SELECT _headers['X-Test'], s
FROM url('http://127.0.0.1:8123/?query=SELECT+1&http_response_headers=%7B%27X-Test%27%3A%27hello%27%7D', LineAsString, 's String')
WHERE _path = '/';

SELECT '-- the query of the AST fuzzer, with the condition negated to get a row';
SELECT empty(_headers['X-Long']), s
FROM url('http://127.0.0.1:8123/?query=SELECT+1&http_response_headers=%7B%27X-Test%27%3A%27hello%27%7D', LineAsString, 's String')
WHERE NOT isNotDistinctFrom(_size, true);

-- The HTTP interface does not tell the size and the modification time of its response.
SELECT '-- _headers before all the other virtual columns';
SELECT _headers['X-Test'], _size IS NULL OR _size > 0, _time IS NULL OR _time > '2000-01-01 00:00:00', _path, _file, s
FROM url('http://127.0.0.1:8123/?query=SELECT+1&http_response_headers=%7B%27X-Test%27%3A%27hello%27%7D', LineAsString, 's String');

-- A read of only virtual columns takes the number of rows from the cache without a request for the
-- data, and `_headers` was empty then, while it is the headers of the response to that request.
-- The cache is used for a URL without the `Last-Modified` header only with the setting below.
SET schema_inference_cache_require_modification_time_for_url = 0;

SELECT '-- the number of rows is in the cache';
SELECT count()
FROM url('http://127.0.0.1:8123/?query=SELECT+1&http_response_headers=%7B%27X-Test%27%3A%27hello%27%7D', LineAsString, 's String');

SELECT '-- only virtual columns, _headers before another virtual column';
SELECT _headers['X-Test'], _path
FROM url('http://127.0.0.1:8123/?query=SELECT+1&http_response_headers=%7B%27X-Test%27%3A%27hello%27%7D', LineAsString, 's String');

SELECT '-- only virtual columns, _headers after another virtual column';
SELECT _path, _headers['X-Test']
FROM url('http://127.0.0.1:8123/?query=SELECT+1&http_response_headers=%7B%27X-Test%27%3A%27hello%27%7D', LineAsString, 's String');
