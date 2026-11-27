#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs S3 support

# The parallel listing of a globbed S3 path (`s3_list_object_parallelism` > 1) must not attach the metadata of the
# listing to the objects when an explicit version is read (`?versionId=`): the listing describes the current
# versions, while the size of the selected version bounds the read and decides if the file is skipped as empty.
# The results must be the same as with the serial listing (`s3_list_object_parallelism` = 1).
#
# The server below mimics S3 for two versioned objects:
# - `data1.csv`: the current version is `9\n`, the version `old` is `1\n2\n` (larger than the current one);
# - `data2.csv`: the current version is empty, the version `old` is `3\n`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

python3 -c "
import sys
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlsplit, parse_qs

OBJECTS = {
    'data1.csv': {None: (b'9\n', '\"cur1\"'), 'old': (b'1\n2\n', '\"old1\"')},
    'data2.csv': {None: (b'', '\"cur2\"'), 'old': (b'3\n', '\"old2\"')},
}

class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *args):
        pass

    def send(self, status, content_type, body, with_body, headers=()):
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(body)))
        for name, value in headers:
            self.send_header(name, value)
        self.end_headers()
        if with_body:
            self.wfile.write(body)

    def list(self, query, with_body):
        prefix = query.get('prefix', [''])[0]
        delimiter = query.get('delimiter', [''])[0]
        after = query.get('continuation-token', query.get('start-after', ['']))[0]
        max_keys = int(query.get('max-keys', ['1000'])[0])
        # All the keys are in the root of the bucket, so no key is ever folded into a common prefix.
        keys = sorted(k for k in OBJECTS if k.startswith(prefix) and k > after
            and not (delimiter and delimiter in k[len(prefix):]))
        truncated = len(keys) > max_keys
        keys = keys[:max_keys]
        body = ('<?xml version=\"1.0\" encoding=\"UTF-8\"?>'
            '<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">'
            '<Name>test</Name><Prefix>' + prefix + '</Prefix>'
            '<KeyCount>' + str(len(keys)) + '</KeyCount><MaxKeys>' + str(max_keys) + '</MaxKeys>'
            '<IsTruncated>' + ('true' if truncated else 'false') + '</IsTruncated>')
        if truncated:
            body += '<NextContinuationToken>' + keys[-1] + '</NextContinuationToken>'
        for key in keys:
            data, etag = OBJECTS[key][None]
            body += ('<Contents><Key>' + key + '</Key><LastModified>2026-11-02T00:00:00.000Z</LastModified>'
                '<ETag>' + etag.replace('\"', '&quot;') + '</ETag><Size>' + str(len(data)) + '</Size>'
                '<StorageClass>STANDARD</StorageClass></Contents>')
        body += '</ListBucketResult>'
        self.send(200, 'application/xml', body.encode(), with_body)

    def respond(self, with_body):
        url = urlsplit(self.path)
        query = parse_qs(url.query, keep_blank_values=True)
        if url.path.rstrip('/') == '/test' and 'list-type' in query:
            self.list(query, with_body)
            return
        key = url.path[len('/test/'):] if url.path.startswith('/test/') else None
        version = query.get('versionId', [None])[0]
        if key not in OBJECTS or version not in OBJECTS[key]:
            self.send(404, 'application/xml', b'', with_body)
            return
        data, etag = OBJECTS[key][version]
        headers = [('Accept-Ranges', 'bytes'), ('Last-Modified', 'Mon, 02 Nov 2026 00:00:00 GMT'), ('ETag', etag)]
        if version is not None:
            headers.append(('x-amz-version-id', version))
        rng = self.headers.get('Range')
        status = 200
        if rng and data:
            begin, end = rng.split('=')[1].split('-')
            begin = int(begin)
            end = min(int(end), len(data) - 1) if end else len(data) - 1
            headers.append(('Content-Range', 'bytes %d-%d/%d' % (begin, end, len(data))))
            data = data[begin:end + 1]
            status = 206
        self.send(status, 'text/csv', data, with_body, headers)

    def do_HEAD(self):
        self.respond(False)

    def do_GET(self):
        self.respond(True)

ThreadingHTTPServer(('127.0.0.1', int(sys.argv[1])), Handler).serve_forever()
" "${HTTP_PORT}" &
HTTP_PID=$!
trap "kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null" EXIT

for _ in $(seq 1 50); do
    curl -s "http://127.0.0.1:$HTTP_PORT/test/data1.csv" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

wildcard_url="http://127.0.0.1:${HTTP_PORT}/test/data*.csv"

for parallelism in 1 8; do
    for prefetch in 0 1; do
        ${CLICKHOUSE_LOCAL} --query "
            SELECT 'current', arraySort(groupArray(x)) FROM s3('${wildcard_url}', 'test', 'testtest', 'CSV', 'x UInt64')
                SETTINGS s3_list_object_parallelism = ${parallelism}, remote_filesystem_read_prefetch = ${prefetch};
            SELECT 'old', arraySort(groupArray(x)) FROM s3('${wildcard_url}?versionId=old', 'test', 'testtest', 'CSV', 'x UInt64')
                SETTINGS s3_list_object_parallelism = ${parallelism}, remote_filesystem_read_prefetch = ${prefetch};
            SELECT 'old, skip empty', arraySort(groupArray(x)) FROM s3('${wildcard_url}?versionId=old', 'test', 'testtest', 'CSV', 'x UInt64')
                SETTINGS s3_list_object_parallelism = ${parallelism}, remote_filesystem_read_prefetch = ${prefetch}, s3_skip_empty_files = 1;
            SELECT 'old, sizes', arraySort(groupUniqArray((_file, _size))) FROM s3('${wildcard_url}?versionId=old', 'test', 'testtest', 'CSV', 'x UInt64')
                SETTINGS s3_list_object_parallelism = ${parallelism}, remote_filesystem_read_prefetch = ${prefetch};"
    done
done
