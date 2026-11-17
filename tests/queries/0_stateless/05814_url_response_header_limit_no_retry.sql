-- A response header that exceeds a limit is not retried: the server sends the same header again.
-- The HTTP interface adds the headers of the `http_response_headers` setting to its response.
-- The requests must be sent by this query: with parallel replicas, `url` becomes `urlCluster`, and the replicas send them.

SELECT s
FROM url('http://127.0.0.1:8123/?query=SELECT+1&http_response_headers=' || encodeURLComponent('{\'X-Long\':\'' || repeat('a', 20000) || '\'}'), LineAsString, 's String')
SETTINGS http_max_field_value_size = 8192, enable_parallel_replicas = 0, log_comment = 'value'; -- { serverError LIMIT_EXCEEDED }

SELECT s
FROM url('http://127.0.0.1:8123/?query=SELECT+1', LineAsString, 's String')
SETTINGS http_max_fields = 2, enable_parallel_replicas = 0, log_comment = 'fields'; -- { serverError LIMIT_EXCEEDED }

SYSTEM FLUSH LOGS query_log;

-- With retries, each query would send `http_max_tries` (10) requests.
SELECT log_comment, ProfileEvents['ReadWriteBufferFromHTTPRequestsSent']
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND log_comment IN ('value', 'fields') AND type != 'QueryStart'
ORDER BY log_comment;
