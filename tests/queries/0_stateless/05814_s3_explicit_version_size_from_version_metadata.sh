#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs S3 support

# A read of an explicit version of an S3 object (`?versionId=`) must use the metadata (the size) of that version,
# not of the current one: the size bounds the read (e.g. with the small-file prefetch), so a historical version that
# is larger than the current one was silently truncated. The same for a wildcard, whose listing describes the current
# versions of the objects.
#
# The server below mimics S3 for one versioned object: the current version is `9\n`, the version `old` is `1\n2\n`.

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
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlsplit, parse_qs

VERSIONS = {None: (b'9\n', '\"cur\"'), 'old': (b'1\n2\n', '\"old\"')}

class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *args):
        pass

    def respond(self, with_body):
        url = urlsplit(self.path)
        query = parse_qs(url.query)
        if url.path.rstrip('/') == '/test' and 'list-type' in query:
            data, etag = VERSIONS[None]
            body = ('<?xml version=\"1.0\" encoding=\"UTF-8\"?>'
                '<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">'
                '<Name>test</Name><Prefix>' + query.get('prefix', [''])[0] + '</Prefix>'
                '<KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>'
                '<Contents><Key>data.csv</Key><LastModified>2026-11-02T00:00:00.000Z</LastModified>'
                '<ETag>&quot;cur&quot;</ETag><Size>' + str(len(data)) + '</Size><StorageClass>STANDARD</StorageClass></Contents>'
                '</ListBucketResult>').encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/xml')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            if with_body:
                self.wfile.write(body)
            return
        version = query.get('versionId', [None])[0]
        if url.path != '/test/data.csv' or version not in VERSIONS:
            self.send_response(404)
            self.send_header('Content-Length', '0')
            self.end_headers()
            return
        data, etag = VERSIONS[version]
        rng = self.headers.get('Range')
        status = 200
        if rng:
            begin, end = rng.split('=')[1].split('-')
            begin = int(begin)
            end = min(int(end), len(data) - 1) if end else len(data) - 1
            content_range = 'bytes %d-%d/%d' % (begin, end, len(data))
            data = data[begin:end + 1]
            status = 206
        self.send_response(status)
        self.send_header('Content-Type', 'text/csv')
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Accept-Ranges', 'bytes')
        self.send_header('Last-Modified', 'Mon, 02 Nov 2026 00:00:00 GMT')
        if status == 206:
            self.send_header('Content-Range', content_range)
        self.send_header('ETag', etag)
        if version is not None:
            self.send_header('x-amz-version-id', version)
        self.end_headers()
        if with_body:
            self.wfile.write(data)

    def do_HEAD(self):
        self.respond(False)

    def do_GET(self):
        self.respond(True)

HTTPServer(('127.0.0.1', int(sys.argv[1])), Handler).serve_forever()
" "${HTTP_PORT}" &
HTTP_PID=$!
trap "kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null" EXIT

for _ in $(seq 1 50); do
    curl -s "http://127.0.0.1:$HTTP_PORT/test/data.csv" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

url="http://127.0.0.1:${HTTP_PORT}/test/data.csv"
wildcard_url="http://127.0.0.1:${HTTP_PORT}/test/data*.csv"

for prefetch in 0 1; do
    ${CLICKHOUSE_LOCAL} --query "
        SELECT 'current', groupArray(x) FROM s3('${url}', 'test', 'testtest', 'CSV', 'x UInt64')
            SETTINGS remote_filesystem_read_prefetch = ${prefetch};
        SELECT 'old', groupArray(x) FROM s3('${url}?versionId=old', 'test', 'testtest', 'CSV', 'x UInt64')
            SETTINGS remote_filesystem_read_prefetch = ${prefetch};
        SELECT 'wildcard current', groupArray(x) FROM s3('${wildcard_url}', 'test', 'testtest', 'CSV', 'x UInt64')
            SETTINGS remote_filesystem_read_prefetch = ${prefetch};
        SELECT 'wildcard old', groupArray(x) FROM s3('${wildcard_url}?versionId=old', 'test', 'testtest', 'CSV', 'x UInt64')
            SETTINGS remote_filesystem_read_prefetch = ${prefetch};"
done
