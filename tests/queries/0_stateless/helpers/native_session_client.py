"""A small native protocol client for tests of session state.

The client uses protocol revision 54449, which is lower than the revision of
the `ResetSession` packet. The server accepts the packet from a client with
any revision, and the tests use this client to show it.

`query` returns the rows as lists of strings. It supports only `String`
result columns, so cast the selected values with `toString` in the tests.
"""

import os
import socket
import uuid

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")
CLICKHOUSE_PORT_TCP = int(os.environ.get("CLICKHOUSE_PORT_TCP", "9000"))
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "default")

CLIENT_NAME = "native session test client"
CLIENT_REVISION = 54449
RESET_SESSION_REVISION = 54493

CLIENT_HELLO = 0
CLIENT_QUERY = 1
CLIENT_DATA = 2
CLIENT_PING = 4
CLIENT_RESET_SESSION = 15

SERVER_HELLO = 0
SERVER_DATA = 1
SERVER_EXCEPTION = 2
SERVER_PROGRESS = 3
SERVER_PONG = 4
SERVER_END_OF_STREAM = 5
SERVER_PROFILE_INFO = 6
SERVER_TOTALS = 7
SERVER_EXTREMES = 8
SERVER_LOG = 10
SERVER_TABLE_COLUMNS = 11


class ServerError(Exception):
    def __init__(self, code, message):
        super().__init__(f"code {code}: {message}")
        self.code = code


class ConnectionClosed(Exception):
    pass


def write_varuint(x, ba):
    while True:
        byte = x & 0x7F
        x >>= 7
        if x:
            ba.append(byte | 0x80)
        else:
            ba.append(byte)
            return


def write_string(s, ba):
    b = s.encode("utf-8")
    write_varuint(len(b), ba)
    ba.extend(b)


class NativeSessionClient:
    def __init__(self, user="default", password="", database=CLICKHOUSE_DATABASE,
                 host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT_TCP):
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = port
        self.sock = None
        self.server_revision = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()

    def connect(self):
        self.sock = socket.create_connection((self.host, self.port), timeout=30)
        ba = bytearray()
        write_varuint(CLIENT_HELLO, ba)
        write_string(CLIENT_NAME, ba)
        write_varuint(1, ba)  # version major
        write_varuint(0, ba)  # version minor
        write_varuint(CLIENT_REVISION, ba)
        write_string(self.database, ba)
        write_string(self.user, ba)
        write_string(self.password, ba)
        self.sock.sendall(ba)

        packet = self.read_varuint()
        if packet == SERVER_EXCEPTION:
            raise self.read_exception()
        assert packet == SERVER_HELLO, f"unexpected packet {packet}"
        self.read_string()  # server name
        self.read_varuint()  # version major
        self.read_varuint()  # version minor
        self.server_revision = self.read_varuint()
        self.read_string()  # time zone
        self.read_string()  # display name
        self.read_varuint()  # version patch

    def close(self):
        if self.sock:
            self.sock.close()
            self.sock = None

    # Reading.

    def read_bytes(self, size):
        res = bytearray()
        while len(res) < size:
            chunk = self.sock.recv(size - len(res))
            if not chunk:
                raise ConnectionClosed("the server closed the connection")
            res.extend(chunk)
        return res

    def read_uint(self, size):
        return int.from_bytes(self.read_bytes(size), "little")

    def read_varuint(self):
        x = 0
        for i in range(10):
            byte = self.read_bytes(1)[0]
            x |= (byte & 0x7F) << (7 * i)
            if not byte & 0x80:
                return x
        return x

    def read_string(self):
        return self.read_bytes(self.read_varuint()).decode("utf-8")

    def read_exception(self):
        code = self.read_uint(4)
        self.read_string()  # exception class name
        message = self.read_string()
        self.read_string()  # stack trace
        has_nested = self.read_uint(1)
        error = ServerError(code, message)
        if has_nested:
            self.read_exception()
        return error

    def read_block(self):
        self.read_string()  # external table name
        # Block info: field numbers and values, terminated by 0.
        while True:
            field = self.read_varuint()
            if field == 0:
                break
            if field == 1:
                self.read_uint(1)  # is_overflows
            elif field == 2:
                self.read_uint(4)  # bucket_num
            else:
                raise AssertionError(f"unknown block info field {field}")
        num_columns = self.read_varuint()
        num_rows = self.read_varuint()
        columns = []
        for _ in range(num_columns):
            self.read_string()  # column name
            column_type = self.read_string()
            if column_type == "String":
                columns.append([self.read_string() for _ in range(num_rows)])
            elif column_type.startswith("Enum8("):
                columns.append([str(self.read_uint(1)) for _ in range(num_rows)])
            else:
                raise AssertionError(f"unsupported column type {column_type}")
        return [list(row) for row in zip(*columns)] if columns else []

    # Requests.

    def send_query(self, sql, end_external_tables=True):
        ba = bytearray()
        query_id = uuid.uuid4().hex
        write_varuint(CLIENT_QUERY, ba)
        write_string(query_id, ba)

        # Client info.
        ba.append(1)  # INITIAL_QUERY
        write_string("", ba)  # initial user
        write_string(query_id, ba)  # initial query id
        write_string("127.0.0.1:0", ba)  # initial address
        ba.extend([0] * 8)  # initial query start time
        ba.append(1)  # interface: TCP
        write_string("", ba)  # OS user
        write_string("", ba)  # client host name
        write_string(CLIENT_NAME, ba)
        write_varuint(1, ba)  # version major
        write_varuint(0, ba)  # version minor
        write_varuint(CLIENT_REVISION, ba)
        write_string("", ba)  # quota key
        write_varuint(0, ba)  # distributed depth
        write_varuint(0, ba)  # version patch
        ba.append(0)  # no OpenTelemetry context

        write_string("", ba)  # end of settings
        write_string("", ba)  # interserver secret
        write_varuint(2, ba)  # stage: complete
        write_varuint(0, ba)  # no compression
        write_string(sql, ba)
        self.sock.sendall(ba)

        if end_external_tables:
            self.send_empty_block()

    def send_empty_block(self):
        """Sends an empty `Data` block. After a query, it ends the external tables."""
        ba = bytearray()
        write_varuint(CLIENT_DATA, ba)
        write_string("", ba)
        write_varuint(1, ba)
        ba.append(0)  # is_overflows
        write_varuint(2, ba)
        ba.extend((-1).to_bytes(4, "little", signed=True))  # bucket_num
        write_varuint(0, ba)
        write_varuint(0, ba)  # columns
        write_varuint(0, ba)  # rows
        self.sock.sendall(ba)

    def receive_result(self):
        rows = []
        while True:
            packet = self.read_varuint()
            if packet == SERVER_DATA:
                rows.extend(self.read_block())
            elif packet in (SERVER_TOTALS, SERVER_EXTREMES, SERVER_LOG):
                self.read_block()
            elif packet == SERVER_TABLE_COLUMNS:
                self.read_string()  # external table name
                self.read_string()  # columns description
            elif packet == SERVER_PROGRESS:
                for _ in range(5):
                    self.read_varuint()
            elif packet == SERVER_PROFILE_INFO:
                self.read_varuint()  # rows
                self.read_varuint()  # blocks
                self.read_varuint()  # bytes
                self.read_uint(1)  # applied limit
                self.read_varuint()  # rows before limit
                self.read_uint(1)  # calculated rows before limit
            elif packet == SERVER_EXCEPTION:
                raise self.read_exception()
            elif packet == SERVER_END_OF_STREAM:
                return rows
            else:
                raise AssertionError(f"unexpected packet {packet}")

    def query(self, sql):
        self.send_query(sql)
        return self.receive_result()

    def value(self, sql):
        rows = self.query(sql)
        assert len(rows) == 1 and len(rows[0]) == 1, f"expected one value, got {rows}"
        return rows[0][0]

    def send_reset_session(self):
        ba = bytearray()
        write_varuint(CLIENT_RESET_SESSION, ba)
        self.sock.sendall(ba)

    def reset_session(self):
        self.send_reset_session()
        packet = self.read_varuint()
        if packet == SERVER_EXCEPTION:
            raise self.read_exception()
        assert packet == SERVER_END_OF_STREAM, f"unexpected packet {packet}"

    def ping(self):
        ba = bytearray()
        write_varuint(CLIENT_PING, ba)
        self.sock.sendall(ba)
        packet = self.read_varuint()
        assert packet == SERVER_PONG, f"unexpected packet {packet}"


def error_name(code):
    """Returns the name of the error code. Uses a new connection, because the failed one can be closed."""
    with NativeSessionClient() as client:
        return client.value(f"SELECT CAST(errorCodeToName({int(code)}) AS String)")


def expect_error(name, func, *args):
    """Calls `func` and checks that it raises a server error with the name `name`."""
    try:
        func(*args)
    except ServerError as e:
        actual = error_name(e.code)
        assert actual == name, f"expected {name}, got {actual}: {e}"
        return
    raise AssertionError(f"expected {name}, got no error")
