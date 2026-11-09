#!/usr/bin/env bash
# Tags: no-fasttest

# A listed S3 key shaped like a URI (`http://example.com/a.csv` in bucket `mydata`) must be read from `mydata`, not from bucket `example.com`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

python3 -c "
import hashlib, re, threading
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, unquote, urlsplit

OBJECTS = {
    'mydata': {'http://example.com/a.csv': b'1,correct_row\n', 'plain/b.csv': b'5,normal_row\n'},
    'example.com': {'a.csv': b'9,WRONG_TWIN_ROW\n'},
}
requested = set()
lock = threading.Lock()

def collapse(key):
    return re.sub('/+', '/', key)

def etag(body):
    return '\"' + hashlib.md5(body).hexdigest() + '\"'

class Handler(BaseHTTPRequestHandler):
    def send(self, code, body=b'', headers=()):
        self.send_response(code)
        for name, value in headers:
            self.send_header(name, value)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        if self.command != 'HEAD':
            self.wfile.write(body)

    def reply(self):
        url = urlsplit(self.path)
        path = unquote(url.path)
        if path == '/ping':
            return self.send(200, b'ok')
        if path == '/buckets':
            with lock:
                return self.send(200, ''.join(b + '\n' for b in sorted(requested)).encode())
        bucket, _, key = path.lstrip('/').partition('/')
        with lock:
            requested.add(bucket)
        objects = OBJECTS.get(bucket, {})
        query = parse_qs(url.query)
        if 'list-type' in query:
            prefix = query.get('prefix', [''])[0]
            contents = ''.join(
                '<Contents><Key>%s</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>%s</ETag><Size>%d</Size></Contents>'
                % (k, etag(v).replace('\"', '&quot;'), len(v))
                for k, v in sorted(objects.items()) if k.startswith(prefix))
            body = ('<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">'
                '<Name>%s</Name><Prefix>%s</Prefix><IsTruncated>false</IsTruncated>%s</ListBucketResult>' % (bucket, prefix, contents)).encode()
            return self.send(200, body, [('Content-Type', 'application/xml')])
        found = [v for k, v in objects.items() if collapse(k) == collapse(key)]
        if not found:
            return self.send(404, b'<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NoSuchKey</Code><Message>not found</Message></Error>',
                [('Content-Type', 'application/xml')])
        body = found[0]
        headers = [('ETag', etag(body)), ('Last-Modified', 'Thu, 01 Jan 2026 00:00:00 GMT')]
        match = re.match(r'bytes=(\d+)-(\d*)', self.headers.get('Range', ''))
        if not match:
            return self.send(200, body, headers)
        start = int(match.group(1))
        end = min(int(match.group(2)) if match.group(2) else len(body) - 1, len(body) - 1)
        if start >= len(body):
            return self.send(416, b'', headers)
        self.send(206, body[start:end + 1], headers + [('Content-Range', 'bytes %d-%d/%d' % (start, end, len(body)))])

    do_GET = do_HEAD = reply

    def log_message(self, *args):
        pass

ThreadingHTTPServer(('127.0.0.1', $PORT), Handler).serve_forever()
" &
SERVER_PID=$!

trap 'kill $SERVER_PID 2>/dev/null' EXIT

for _ in {1..100}; do
    if [ "$(${CLICKHOUSE_CURL} -o /dev/null -w "%{http_code}" "http://127.0.0.1:${PORT}/ping")" = "200" ]; then
        break
    fi
    sleep 0.1
done

${CLICKHOUSE_LOCAL} --query "SELECT _path, c1, c2 FROM s3('http://127.0.0.1:${PORT}/mydata/**', NOSIGN, 'CSV', 'c1 Int64, c2 String') ORDER BY _path" 2>&1

${CLICKHOUSE_CURL} "http://127.0.0.1:${PORT}/buckets"
