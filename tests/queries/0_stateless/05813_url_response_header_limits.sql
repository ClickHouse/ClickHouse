-- The `url` table function checks the header of the response against the limits of the
-- `http_max_fields`, `http_max_field_name_size` and `http_max_field_value_size` settings.
-- The HTTP interface adds the headers of the `http_response_headers` setting to its response.

-- A field value of 20000 bytes is more than the 8 KiB default of `Poco`.
SELECT s, length(_headers['X-Long'])
FROM url('http://127.0.0.1:8123/?query=SELECT+1&http_response_headers=' || encodeURLComponent('{\'X-Long\':\'' || repeat('a', 20000) || '\'}'), LineAsString, 's String');

SELECT s
FROM url('http://127.0.0.1:8123/?query=SELECT+1&http_response_headers=' || encodeURLComponent('{\'X-Long\':\'' || repeat('a', 20000) || '\'}'), LineAsString, 's String')
SETTINGS http_max_field_value_size = 8192, http_max_tries = 1; -- { serverError LIMIT_EXCEEDED }

SELECT s
FROM url('http://127.0.0.1:8123/?query=SELECT+1', LineAsString, 's String')
SETTINGS http_max_field_name_size = 4, http_max_tries = 1; -- { serverError LIMIT_EXCEEDED }

SELECT s
FROM url('http://127.0.0.1:8123/?query=SELECT+1', LineAsString, 's String')
SETTINGS http_max_fields = 2, http_max_tries = 1; -- { serverError LIMIT_EXCEEDED }
