"""A minimal client of the Kafka protocol for the tests of the Kafka-compatible interface of the server.

It sends the requests of the versions without the compact encodings, one request at a time,
and lets a test send malformed or incomplete requests, which a real client does not do.
"""

import socket
import struct
import zlib


class Parser:
    def __init__(self, data):
        self.data = data
        self.pos = 0

    def unpack(self, fmt):
        values = struct.unpack_from(fmt, self.data, self.pos)
        self.pos += struct.calcsize(fmt)
        return values[0]

    def int8(self):
        return self.unpack(">b")

    def int16(self):
        return self.unpack(">h")

    def int32(self):
        return self.unpack(">i")

    def int64(self):
        return self.unpack(">q")

    def raw(self, size):
        res = self.data[self.pos : self.pos + size]
        assert len(res) == size
        self.pos += size
        return res

    def string(self):
        size = self.int16()
        return None if size < 0 else self.raw(size).decode()

    def bytes(self):
        size = self.int32()
        return None if size < 0 else self.raw(size)

    def varint(self):
        result = 0
        shift = 0
        while True:
            byte = self.raw(1)[0]
            result |= (byte & 0x7F) << shift
            if not byte & 0x80:
                break
            shift += 7
        return (result >> 1) ^ -(result & 1)

    def varbytes(self):
        size = self.varint()
        return None if size < 0 else self.raw(size)

    def eof(self):
        return self.pos == len(self.data)


def string(value):
    if value is None:
        return struct.pack(">h", -1)
    value = value.encode() if isinstance(value, str) else value
    return struct.pack(">h", len(value)) + value


def bytes_(value):
    if value is None:
        return struct.pack(">i", -1)
    return struct.pack(">i", len(value)) + value


def array(items):
    return struct.pack(">i", len(items)) + b"".join(items)


def varint(value):
    value = (value << 1) ^ (value >> 63)
    res = b""
    while value >= 0x80:
        res += bytes([(value & 0x7F) | 0x80])
        value >>= 7
    return res + bytes([value])


def varbytes(value):
    if value is None:
        return varint(-1)
    return varint(len(value)) + value


def _crc32c_table():
    table = []
    for i in range(256):
        crc = i
        for _ in range(8):
            crc = (crc >> 1) ^ 0x82F63B78 if crc & 1 else crc >> 1
        table.append(crc)
    return table


_CRC32C_TABLE = _crc32c_table()


def crc32c(data):
    crc = 0xFFFFFFFF
    for byte in data:
        crc = _CRC32C_TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


def gzip_compress(data):
    compressor = zlib.compressobj(6, zlib.DEFLATED, 31)
    return compressor.compress(data) + compressor.flush()


def record_batch(values, compress=False):
    """A record batch of the message format v2 with the values of the records, optionally compressed with gzip."""
    records = b""
    for i, value in enumerate(values):
        record = b"\x00" + varint(0) + varint(i) + varbytes(None) + varbytes(value) + varint(0)
        records += varint(len(record)) + record
    if compress:
        records = gzip_compress(records)

    body = struct.pack(
        ">hiqqqhii",
        1 if compress else 0,  # attributes
        len(values) - 1,  # last offset delta
        0,  # base timestamp
        0,  # max timestamp
        -1,  # producer id
        -1,  # producer epoch
        -1,  # base sequence
        len(values),
    )
    body += records
    # The partition leader epoch, the magic byte, and the checksum of the rest.
    batch = struct.pack(">ibI", 0, 2, crc32c(body)) + body
    return struct.pack(">qi", 0, len(batch)) + batch


def parse_record_batches(data):
    """Returns the batches as lists of (offset, value), and the offset after the last batch."""
    batches = []
    next_offset = None
    parser = Parser(data)
    while not parser.eof():
        base_offset = parser.int64()
        batch = Parser(parser.raw(parser.int32()))
        batch.int32()  # partition leader epoch
        assert batch.int8() == 2
        batch.raw(4)  # crc
        attributes = batch.int16()
        assert attributes & 7 == 0
        last_offset_delta = batch.int32()
        batch.raw(8 + 8 + 8 + 2 + 4)
        records = []
        for _ in range(batch.int32()):
            record = Parser(batch.raw(batch.varint()))
            record.int8()
            record.varint()  # timestamp delta
            offset = base_offset + record.varint()
            record.varbytes()  # key
            records.append((offset, record.varbytes()))
        batches.append(records)
        next_offset = base_offset + last_offset_delta + 1
    return batches, next_offset


class Client:
    def __init__(self, host, port, user=None, password=""):
        self.sock = socket.create_connection((host, port), timeout=120)
        self.correlation_id = 0
        if user is not None:
            self.authenticate(user, password)

    def close(self):
        self.sock.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def recv_exactly(self, size):
        data = b""
        while len(data) < size:
            chunk = self.sock.recv(size - len(data))
            if not chunk:
                raise ConnectionError("connection closed")
            data += chunk
        return data

    def is_closed_by_server(self):
        """Waits until the server closes the connection. Returns False if it sends something or keeps it open."""
        try:
            return self.sock.recv(1) == b""
        except ConnectionError:
            return True
        except socket.timeout:
            return False

    def header(self, api_key, api_version):
        self.correlation_id += 1
        return struct.pack(">hhi", api_key, api_version, self.correlation_id) + string("test")

    def send_message(self, message):
        self.sock.sendall(struct.pack(">i", len(message)) + message)

    def request(self, api_key, api_version, body=b""):
        self.send_message(self.header(api_key, api_version) + body)
        response = self.recv_exactly(struct.unpack(">i", self.recv_exactly(4))[0])
        assert struct.unpack_from(">i", response, 0)[0] == self.correlation_id
        return Parser(response[4:])

    def authenticate(self, user, password):
        response = self.request(17, 1, string("PLAIN"))
        assert response.int16() == 0
        response = self.request(36, 0, bytes_(b"\x00" + user.encode() + b"\x00" + password.encode()))
        error = response.int16()
        assert error == 0, f"authentication failed: {error}"

    def api_versions(self):
        return self.request(18, 0).int16()

    def fetch(self, topic, partition, offset, max_bytes=1 << 20, max_wait_ms=0):
        """Returns (error, high watermark, batches, the offset after the last batch)."""
        partitions = array([struct.pack(">iqi", partition, offset, max_bytes)])
        body = struct.pack(">iiiib", -1, max_wait_ms, 1, max_bytes, 0) + array([string(topic) + partitions])
        response = self.request(1, 4, body)
        response.int32()  # throttle time
        assert response.int32() == 1
        response.string()
        assert response.int32() == 1
        response.int32()  # partition
        error = response.int16()
        high_watermark = response.int64()
        response.int64()  # last stable offset
        assert response.int32() == 0  # aborted transactions
        batches, next_offset = parse_record_batches(response.bytes() or b"")
        return error, high_watermark, batches, next_offset

    def list_offset(self, topic, partition, timestamp):
        """Returns (error, offset). The timestamp -1 is for the end offset, -2 for the first one."""
        partitions = array([struct.pack(">iq", partition, timestamp)])
        response = self.request(2, 1, struct.pack(">i", -1) + array([string(topic) + partitions]))
        assert response.int32() == 1
        response.string()
        assert response.int32() == 1
        response.int32()  # partition
        error = response.int16()
        response.int64()  # timestamp
        return error, response.int64()

    def produce(self, topic, partition, records):
        """Returns the error code."""
        partitions = array([struct.pack(">i", partition) + bytes_(records)])
        body = string(None) + struct.pack(">hi", 1, 30000) + array([string(topic) + partitions])
        response = self.request(0, 3, body)
        assert response.int32() == 1
        response.string()
        assert response.int32() == 1
        response.int32()  # partition
        return response.int16()

    def join_group(self, group, member_id="", instance_id=None, session_timeout_ms=30000, protocols=None):
        """Returns (error, generation, leader, member id)."""
        protocols = protocols or [("range", b"metadata")]
        body = string(group) + struct.pack(">ii", session_timeout_ms, 30000) + string(member_id) + string(instance_id)
        body += string("consumer") + array([string(name) + bytes_(metadata) for name, metadata in protocols])
        response = self.request(11, 5, body)
        response.int32()  # throttle time
        error = response.int16()
        generation = response.int32()
        response.string()  # protocol name
        leader = response.string()
        return error, generation, leader, response.string()

    def sync_group(self, group, generation, member_id, instance_id=None, assignments=None):
        """Returns (error, assignment)."""
        body = string(group) + struct.pack(">i", generation) + string(member_id) + string(instance_id)
        body += array([string(member) + bytes_(assignment) for member, assignment in (assignments or [])])
        response = self.request(14, 3, body)
        response.int32()  # throttle time
        error = response.int16()
        return error, response.bytes()

    def heartbeat(self, group, generation, member_id, instance_id=None):
        body = string(group) + struct.pack(">i", generation) + string(member_id) + string(instance_id)
        response = self.request(12, 3, body)
        response.int32()  # throttle time
        return response.int16()

    def leave_group(self, group, member_id):
        return self.request(13, 0, string(group) + string(member_id)).int16()

    def offset_commit(self, group, topic, partition, offset, generation=-1, member_id=""):
        partitions = array([struct.pack(">iq", partition, offset) + string("")])
        body = string(group) + struct.pack(">i", generation) + string(member_id) + struct.pack(">q", -1)
        response = self.request(8, 2, body + array([string(topic) + partitions]))
        assert response.int32() == 1
        response.string()
        assert response.int32() == 1
        response.int32()  # partition
        return response.int16()

    def list_groups(self):
        """Returns (error, the ids of the groups)."""
        response = self.request(16, 0)
        error = response.int16()
        groups = []
        for _ in range(response.int32()):
            groups.append(response.string())
            response.string()  # protocol type
        return error, groups

    def describe_group(self, group):
        """Returns (error, state, the ids of the members)."""
        response = self.request(15, 0, array([string(group)]))
        assert response.int32() == 1
        error = response.int16()
        response.string()  # group id
        state = response.string()
        response.string()  # protocol type
        response.string()  # protocol name
        members = []
        for _ in range(response.int32()):
            members.append(response.string())
            response.string()  # client id
            response.string()  # client host
            response.bytes()  # metadata
            response.bytes()  # assignment
        return error, state, members
