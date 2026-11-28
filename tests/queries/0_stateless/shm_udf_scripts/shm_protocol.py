# The control protocol of the shared-memory executable UDF, from the side of the commands here: the
# protocol version, the statuses of an answer, the ClickHouse native binary encoding of its values,
# and the handling of a request - reading it, answering it in the region, and the response frame.
#
# Protocol (all control values use the ClickHouse native binary encoding):
#   server -> stdin : varint version, varint request id, varint path length + path bytes,
#                     varint input offset, varint input size
#   stdout <- server: the request id echoed back, varint status (0 = ok), then on success
#                     varint output offset + varint output size; status 2 asks the server for a
#                     larger region and is followed by the varint total size needed; any other
#                     status is followed by a length-prefixed error message
# The bulk data lives in the shared-memory file at the given path; the pipes carry only
# these small control commands. When stdin reaches EOF the process exits.

import collections
import mmap
import os
import sys

PROTOCOL_VERSION = 1
STATUS_OK = 0
STATUS_ERROR = 1
STATUS_NEED_MORE_SPACE = 2


def read_varint(stream):
    result = 0
    shift = 0
    while True:
        chunk = stream.read(1)
        if not chunk:
            return None
        byte = chunk[0]
        result |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return result
        shift += 7


def encode_varint(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def encode_string(text):
    encoded = text.encode("utf-8")
    return encode_varint(len(encoded)) + encoded


Request = collections.namedtuple("Request", "version request_id path input_offset input_size")


def read_request(stream):
    """The next request, or None once stdin is closed."""
    version = read_varint(stream)
    if version is None:
        return None
    request_id = read_varint(stream)
    path_length = read_varint(stream)
    path = stream.read(path_length).decode("utf-8") if path_length is not None else ""
    input_offset = read_varint(stream)
    input_size = read_varint(stream)
    return Request(version, request_id, path, input_offset, input_size)


class NeedMoreSpace(Exception):
    def __init__(self, required_size):
        super().__init__(f"the shared-memory region must be at least {required_size} bytes")
        self.required_size = required_size


def place_answer(region, offset, output):
    """Writes the answer at `offset` of the region and returns where it is. Only the server can
    resize the region: if the answer does not fit, it is asked for one that does, and re-sends the
    same request over the larger mapping."""
    if offset + len(output) > len(region):
        raise NeedMoreSpace(offset + len(output))
    region[offset : offset + len(output)] = bytes(output)
    region.flush()
    return offset, len(output)


def key_row(row):
    return b"Key " + row + b"\n"


def pid_row(row):
    # The pid of the process, so that a test can tell a reused worker from a fresh one.
    return str(os.getpid()).encode("ascii") + b"\n"


def answer_rows(answer_row):
    """An answer to `TabSeparated` input - one `UInt64` per line - that answers every row with
    `answer_row` of it, right after the input."""

    def answer(input_data, region):
        output = b"".join(answer_row(row) for row in input_data.split(b"\n") if row != b"")
        return place_answer(region, len(input_data), output)

    return answer


def respond(request, answer, before_mapping=None, after_answer=None):
    """Handles one request and returns its response frame. `answer(input_data, region)` writes the
    answer into the mapped region and returns its offset and size. `before_mapping(fd)` and
    `after_answer(fd, offset, size)` are what the command does to the region's file around that."""
    try:
        if request.version != PROTOCOL_VERSION:
            raise ValueError(f"unsupported protocol version {request.version}")

        fd = os.open(request.path, os.O_RDWR)
        try:
            if before_mapping is not None:
                before_mapping(fd)
            region = mmap.mmap(fd, 0)
            try:
                input_data = region[request.input_offset : request.input_offset + request.input_size]
                output_offset, output_size = answer(input_data, region)
            finally:
                region.close()
            if after_answer is not None:
                after_answer(fd, output_offset, output_size)
        finally:
            os.close(fd)

        return ok_frame(request.request_id, output_offset, output_size)
    except NeedMoreSpace as need_more_space:
        return encode_varint(request.request_id) + encode_varint(STATUS_NEED_MORE_SPACE) + encode_varint(need_more_space.required_size)
    except Exception as exception:  # noqa: BLE001
        return error_frame(request.request_id, str(exception))


def ok_frame(request_id, output_offset, output_size):
    return encode_varint(request_id) + encode_varint(STATUS_OK) + encode_varint(output_offset) + encode_varint(output_size)


def error_frame(request_id, message):
    return encode_varint(request_id) + encode_varint(STATUS_ERROR) + encode_string(message)


def serve(answer, before_mapping=None, after_answer=None, after_frame=b""):
    """Answers requests until stdin is closed (see `respond`). Every response frame goes out in one
    write, together with `after_frame` - bytes past it that are part of no frame: written field by
    field, they would go out in separate writes wherever `stdout` is unbuffered (`PYTHONUNBUFFERED`,
    set in CI), and the server could finish the invocation and pronounce the worker clean before the
    stray bytes arrived."""
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer
    while True:
        request = read_request(stdin)
        if request is None:
            return
        stdout.write(respond(request, answer, before_mapping, after_answer) + after_frame)
        stdout.flush()
