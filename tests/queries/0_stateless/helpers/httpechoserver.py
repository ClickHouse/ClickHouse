#!/usr/bin/env python3

import sys
import os
import time
import subprocess
import threading
from io import StringIO, SEEK_END
from http.server import BaseHTTPRequestHandler, HTTPServer

CLICKHOUSE_HOST = os.environ.get('CLICKHOUSE_HOST', '127.0.0.1')
CLICKHOUSE_PORT_HTTP = os.environ.get('CLICKHOUSE_PORT_HTTP', '8123')

# IP-address of this host accessible from outside world.
HTTP_SERVER_HOST = os.environ.get('HTTP_SERVER_HOST', subprocess.check_output(['hostname', '-i']).decode('utf-8').strip())
HTTP_SERVER_PORT = int(os.environ.get('CLICKHOUSE_TEST_HOST_EXPOSED_PORT', 51234))

# IP address and port of the HTTP server started from this script.
HTTP_SERVER_ADDRESS = (HTTP_SERVER_HOST, HTTP_SERVER_PORT)
HTTP_SERVER_URL_STR = 'http://' + ':'.join(str(s) for s in HTTP_SERVER_ADDRESS) + "/"

ostream = StringIO()
istream = sys.stdout

class EchoCSVHTTPServer(BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()

    def do_GET(self):
        self._set_headers()
        with open(CSV_DATA, 'r') as fl:
            ostream.seek(0)
            for row in ostream:
                self.wfile.write(row + '\n')
        return

    def read_chunk(self):
        msg = ''
        while True:
            sym = self.rfile.read(1)
            if sym == '':
                break
            msg += sym.decode('utf-8')
            if msg.endswith('\r\n'):
                break
        length = int(msg[:-2], 16)
        if length == 0:
            return ''
        content = self.rfile.read(length)
        self.rfile.read(2) # read sep \r\n
        return content.decode('utf-8')

    def do_POST(self):
        while True:
            chunk = self.read_chunk()
            if not chunk:
                break
            istream.write(chunk)
            istream.flush()
        text = ""
        self._set_headers()
        self.wfile.write("ok")

    def log_message(self, format, *args):
        return

def start_server(requests_amount, test_data="Hello,2,-2,7.7\nWorld,2,-5,8.8"):
    ostream = StringIO(test_data.decode("utf-8"))

    httpd = HTTPServer(HTTP_SERVER_ADDRESS, EchoCSVHTTPServer)

    def real_func():
        for i in range(requests_amount):
            httpd.handle_request()

    t = threading.Thread(target=real_func)
    return t

def run(requests_amount=1):
    t = start_server(requests_amount)
    t.start()
    t.join()

if __name__ == "__main__":
    exception_text = ''
    for i in range(1, 5):
        try:
            run(int(sys.argv[1]) if len(sys.argv) > 1 else 1)
            break
        except Exception as ex:
            exception_text = str(ex)
            time.sleep(1)

    if exception_text:
        print("Exception: {}".format(exception_text), file=sys.stderr)
        os._exit(1)

