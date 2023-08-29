import socket
import os
import uuid
import struct

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT_TCP", "900000"))
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "default")
CLIENT_NAME = "simple native protocol"


def writeVarUInt(x, ba):
    for _ in range(0, 9):
        byte = x & 0x7F
        if x > 0x7F:
            byte |= 0x80

        ba.append(byte)

        x >>= 7
        if x == 0:
            return


def writeStringBinary(s, ba):
    b = bytes(s, "utf-8")
    writeVarUInt(len(s), ba)
    ba.extend(b)


def serializeClientInfo(ba, query_id):
    writeStringBinary("default", ba)  # initial_user
    writeStringBinary(query_id, ba)  # initial_query_id
    writeStringBinary("127.0.0.1:9000", ba)  # initial_address
    ba.extend([0] * 8)  # initial_query_start_time_microseconds
    ba.append(1)  # TCP
    writeStringBinary("os_user", ba)  # os_user
    writeStringBinary("client_hostname", ba)  # client_hostname
    writeStringBinary(CLIENT_NAME, ba)  # client_name
    writeVarUInt(21, ba)
    writeVarUInt(9, ba)
    writeVarUInt(54449, ba)
    writeStringBinary("", ba)  # quota_key
    writeVarUInt(0, ba)  # distributed_depth
    writeVarUInt(1, ba)  # client_version_patch
    ba.append(0)  # No telemetry


def serializeBlockInfo(ba):
    writeVarUInt(1, ba)  # 1
    ba.append(0)  # is_overflows
    writeVarUInt(2, ba)  # 2
    writeVarUInt(0, ba)  # 0
    ba.extend([0] * 4)  # bucket_num


def assertPacket(packet, expected):
    assert packet == expected, "Got: {}, expected: {}".format(packet, expected)


class Data(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value


class TCPClient(object):
    def __init__(self, timeout=30):
        self.timeout = timeout
        self.socket = None

    def __enter__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(self.timeout)
        self.socket.connect((CLICKHOUSE_HOST, CLICKHOUSE_PORT))

        self.sendHello()
        self.receiveHello()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.socket:
            self.socket.close()

    def readStrict(self, size=1):
        res = bytearray()
        while size:
            cur = self.socket.recv(size)
            # if not res:
            #     raise "Socket is closed"
            size -= len(cur)
            res.extend(cur)

        return res

    def readUInt(self, size=1):
        res = self.readStrict(size)
        val = 0
        for i in range(len(res)):
            val += res[i] << (i * 8)
        return val

    def readUInt8(self):
        return self.readUInt()

    def readUInt16(self):
        return self.readUInt(2)

    def readUInt32(self):
        return self.readUInt(4)

    def readUInt64(self):
        return self.readUInt(8)

    def readFloat16(self):
        return struct.unpack("e", self.readStrict(2))

    def readFloat32(self):
        return struct.unpack("f", self.readStrict(4))

    def readFloat64(self):
        return struct.unpack("d", self.readStrict(8))

    def readVarUInt(self):
        x = 0
        for i in range(9):
            byte = self.readStrict()[0]
            x |= (byte & 0x7F) << (7 * i)

            if not byte & 0x80:
                return x

        return x

    def readStringBinary(self):
        size = self.readVarUInt()
        s = self.readStrict(size)
        return s.decode("utf-8")

    def send(self, byte_array):
        self.socket.sendall(byte_array)

    def sendHello(self):
        ba = bytearray()
        writeVarUInt(0, ba)  # Hello
        writeStringBinary(CLIENT_NAME, ba)
        writeVarUInt(21, ba)
        writeVarUInt(9, ba)
        writeVarUInt(54449, ba)
        writeStringBinary(CLICKHOUSE_DATABASE, ba)  # database
        writeStringBinary("default", ba)  # user
        writeStringBinary("", ba)  # pwd
        self.send(ba)

    def receiveHello(self):
        p_type = self.readVarUInt()
        assert p_type == 0  # Hello
        _server_name = self.readStringBinary()
        _server_version_major = self.readVarUInt()
        _server_version_minor = self.readVarUInt()
        _server_revision = self.readVarUInt()
        _server_timezone = self.readStringBinary()
        _server_display_name = self.readStringBinary()
        _server_version_patch = self.readVarUInt()

    def sendQuery(self, query, settings=None):
        if settings == None:
            settings = {}  # No settings

        ba = bytearray()
        query_id = uuid.uuid4().hex
        writeVarUInt(1, ba)  # query
        writeStringBinary(query_id, ba)

        ba.append(1)  # INITIAL_QUERY

        # client info
        serializeClientInfo(ba, query_id)

        # Settings
        for key, value in settings.items():
            writeStringBinary(key, ba)
            writeVarUInt(1, ba)  # is_important
            writeStringBinary(str(value), ba)
        writeStringBinary("", ba)  # End of settings

        writeStringBinary("", ba)  # No interserver secret
        writeVarUInt(2, ba)  # Stage - Complete
        ba.append(0)  # No compression
        writeStringBinary(query, ba)  # query, finally
        self.send(ba)

    def sendEmptyBlock(self):
        ba = bytearray()
        writeVarUInt(2, ba)  # Data
        writeStringBinary("", ba)
        serializeBlockInfo(ba)
        writeVarUInt(0, ba)  # rows
        writeVarUInt(0, ba)  # columns
        self.send(ba)

    def readException(self):
        code = self.readUInt32()
        _name = self.readStringBinary()
        text = self.readStringBinary()
        self.readStringBinary()  # trace
        assertPacket(self.readUInt8(), 0)  # has_nested
        return "code {}: {}".format(code, text.replace("DB::Exception:", ""))

    def readPacketType(self):
        packet_type = self.readVarUInt()
        if packet_type == 2:  # Exception
            raise RuntimeError(self.readException())

        return packet_type

    def readResponse(self):
        packet_type = self.readPacketType()
        if packet_type == 1:  # Data
            return None
        if packet_type == 3:  # Progress
            return None
        if packet_type == 5:  # End stream
            return None

        raise RuntimeError("Unexpected packet: {}".format(packet_type))

    def readProgressData(self):
        read_rows = self.readVarUInt()
        read_bytes = self.readVarUInt()
        total_rows_to_read = self.readVarUInt()
        written_rows = self.readVarUInt()
        written_bytes = self.readVarUInt()

        return read_rows, read_bytes, total_rows_to_read, written_rows, written_bytes

    def readProgress(self):
        packet_type = self.readPacketType()
        if packet_type == 5:  # End stream
            return None
        assertPacket(packet_type, 3)  # Progress
        return self.readProgressData()

    def readHeaderInfo(self):
        self.readStringBinary()  # external table name
        # BlockInfo
        assertPacket(self.readVarUInt(), 1)  # field number 1
        assertPacket(self.readUInt8(), 0)  # is_overflows
        assertPacket(self.readVarUInt(), 2)  # field number 2
        assertPacket(self.readUInt32(), 4294967295)  # bucket_num
        assertPacket(self.readVarUInt(), 0)  # 0
        columns = self.readVarUInt()  # rows
        rows = self.readVarUInt()  # columns

        return columns, rows

    def readHeader(self):
        packet_type = self.readPacketType()
        assertPacket(packet_type, 1)  # Data

        columns, rows = self.readHeaderInfo()
        print("Rows {} Columns {}".format(rows, columns))
        for _ in range(columns):
            col_name = self.readStringBinary()
            type_name = self.readStringBinary()
            print("Column {} type {}".format(col_name, type_name))

    def readRow(self, row_type, rows):
        supported_row_types = {
            "UInt8": self.readUInt8,
            "UInt16": self.readUInt16,
            "UInt32": self.readUInt32,
            "UInt64": self.readUInt64,
            "Float16": self.readFloat16,
            "Float32": self.readFloat32,
            "Float64": self.readFloat64,
        }
        if row_type in supported_row_types:
            read_type = supported_row_types[row_type]
            row = [read_type() for _ in range(rows)]
            return row
        else:
            raise RuntimeError(
                "Current python version of tcp client doesn't support the following type of row: {}".format(
                    row_type
                )
            )

    def readDataWithoutProgress(self, need_print_info=True):
        packet_type = self.readPacketType()
        while packet_type == 3:  # Progress
            self.readProgressData()
            packet_type = self.readPacketType()

        if packet_type == 5:  # End stream
            return None
        assertPacket(packet_type, 1)  # Data

        columns, rows = self.readHeaderInfo()
        data = []
        if need_print_info:
            print("Rows {} Columns {}".format(rows, columns))

        for _ in range(columns):
            col_name = self.readStringBinary()
            type_name = self.readStringBinary()
            if need_print_info:
                print("Column {} type {}".format(col_name, type_name))

            data.append(Data(col_name, self.readRow(type_name, rows)))

        return data
