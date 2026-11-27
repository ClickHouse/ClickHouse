#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs S3 support

# A `{...}` group of a glob can span '/' even when every path component holds as many '{' as '}': in
# `root/}{ab/cd}{/*.csv` the group is `{ab/cd}`, and the glob matches `root/}ab/cd{/x.csv`. The parallel listing
# of a globbed S3 path (`s3_list_object_parallelism` > 1) prunes the "directories" per path component, which is
# not possible for such a glob: it would compare the directory `}ab` with the literal component `}{ab` and skip
# the matching files. The results must be the same as with the serial listing (`s3_list_object_parallelism` = 1).
#
# A key with '{' cannot be written through the `s3` table function (such a path is taken for a glob), so the
# server below mimics the listing and the reading of S3 for a fixed set of objects.

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
from urllib.parse import urlsplit, parse_qs, unquote
from xml.sax.saxutils import escape

OBJECTS = {
    'root/plain/a.csv': b'1\n',
    'root/}ab/cd{/x.csv': b'10\n20\n',
    'root/}ab/cd{/y.csv': b'30\n',
    'root/}ab/cd{/z.tsv': b'100\n',
    'root/}ab/other/x.csv': b'1000\n',
    'root/}zz/q{/v.csv': b'10000\n',
    'root/}{ab/cd}{/x.csv': b'100000\n',
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
        # As S3 does: the keys below the prefix are grouped by the delimiter into common prefixes, the keys and
        # the common prefixes are returned together in the order of the keys, and both count towards the limit.
        # The continuation token is the last key that the page covers.
        entries = []
        truncated = False
        last = ''
        for key in sorted(k for k in OBJECTS if k.startswith(prefix) and k > after):
            pos = key.find(delimiter, len(prefix)) if delimiter else -1
            entry = (key[:pos + len(delimiter)], True) if pos != -1 else (key, False)
            if not entries or entries[-1] != entry:
                if len(entries) == max_keys:
                    truncated = True
                    break
                entries.append(entry)
            last = key
        body = ('<?xml version=\"1.0\" encoding=\"UTF-8\"?>'
            '<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">'
            '<Name>test</Name><Prefix>' + escape(prefix) + '</Prefix>'
            '<KeyCount>' + str(len(entries)) + '</KeyCount><MaxKeys>' + str(max_keys) + '</MaxKeys>'
            '<IsTruncated>' + ('true' if truncated else 'false') + '</IsTruncated>')
        if delimiter:
            body += '<Delimiter>' + escape(delimiter) + '</Delimiter>'
        if truncated:
            body += '<NextContinuationToken>' + escape(last) + '</NextContinuationToken>'
        for name, is_common_prefix in entries:
            if is_common_prefix:
                body += '<CommonPrefixes><Prefix>' + escape(name) + '</Prefix></CommonPrefixes>'
            else:
                body += ('<Contents><Key>' + escape(name) + '</Key><LastModified>2026-11-26T00:00:00.000Z</LastModified>'
                    '<ETag>&quot;etag&quot;</ETag><Size>' + str(len(OBJECTS[name])) + '</Size>'
                    '<StorageClass>STANDARD</StorageClass></Contents>')
        body += '</ListBucketResult>'
        self.send(200, 'application/xml', body.encode(), with_body)

    def respond(self, with_body):
        url = urlsplit(self.path)
        query = parse_qs(url.query, keep_blank_values=True)
        path = unquote(url.path)
        if path.rstrip('/') == '/test' and 'list-type' in query:
            self.list(query, with_body)
            return
        key = path[len('/test/'):] if path.startswith('/test/') else None
        if key not in OBJECTS:
            self.send(404, 'application/xml', b'', with_body)
            return
        data = OBJECTS[key]
        headers = [('Accept-Ranges', 'bytes'), ('Last-Modified', 'Thu, 26 Nov 2026 00:00:00 GMT'), ('ETag', '\"etag\"')]
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
    curl -s "http://127.0.0.1:$HTTP_PORT/test/root/plain/a.csv" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

# The first two globs have a group that spans '/': `{ab/cd}` and `{ab/cd,zz/q}`. The third one, a glob whose
# wildcards are confined to path components, is listed in parallel: it shows that the server does serve
# the walk over the common prefixes.
for glob in 'root/}{ab/cd}{/*.csv' 'root/}{ab/cd,zz/q}{/*.csv' 'root/*/*/*.csv'; do
    for parallelism in 1 8; do
        echo "${glob}, parallelism ${parallelism}"
        ${CLICKHOUSE_LOCAL} --query "
            SELECT _path, sum(x) FROM s3('http://127.0.0.1:${HTTP_PORT}/test/${glob}', 'test', 'testtest', 'CSV', 'x UInt64')
            GROUP BY _path ORDER BY _path
            SETTINGS s3_list_object_parallelism = ${parallelism}, use_hive_partitioning = 0;"
    done
done
