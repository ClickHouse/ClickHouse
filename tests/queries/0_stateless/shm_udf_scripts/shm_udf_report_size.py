#!/usr/bin/python3

# A pooled shared-memory UDF that answers every row with the size of its region's file, writes the
# answer at the very end of that file, and, once, extends the file after answering
# (`--extend-to BYTES`). The next answer then lies past what the server mapped when it created the
# region: a server that brought its mapping up to the file reads it, one that kept working on the
# part it had mapped rejects the offset. Otherwise identical to shm_udf.py.
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

import mmap
import os
import sys

PROTOCOL_VERSION = 1
STATUS_OK = 0
STATUS_ERROR = 1
STATUS_NEED_MORE_SPACE = 2


class NeedMoreSpace(Exception):
    def __init__(self, required_size):
        super().__init__(
            f"the shared-memory region must be at least {required_size} bytes"
        )
        self.required_size = required_size


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


def write_varint(stream, value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            break
    stream.write(bytes(out))


def write_string_binary(stream, text):
    encoded = text.encode("utf-8")
    write_varint(stream, len(encoded))
    stream.write(encoded)


def process(input_data, region, region_size):
    output = bytearray()
    for line in input_data.split(b"\n"):
        if line == b"":
            continue
        output += str(region_size).encode() + b"\n"

    # The answer goes at the very end of the file rather than right after the input: after the
    # extension below that is past what the server mapped when it created the region, and an
    # answer there is only readable to a server that maps the whole file.
    if len(input_data) + len(output) > region_size:
        raise NeedMoreSpace(len(input_data) + len(output))
    output_offset = region_size - len(output)

    region[output_offset : output_offset + len(output)] = bytes(output)
    region.flush()
    return output_offset, len(output)


def main():
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer

    while True:
        version = read_varint(stdin)
        if version is None:
            break  # stdin closed -> exit
        request_id = read_varint(stdin)

        path_length = read_varint(stdin)
        path = stdin.read(path_length).decode("utf-8")
        input_offset = read_varint(stdin)
        input_size = read_varint(stdin)

        try:
            if version != PROTOCOL_VERSION:
                raise ValueError(f"unsupported protocol version {version}")

            fd = os.open(path, os.O_RDWR)
            try:
                region = mmap.mmap(fd, 0)
                try:
                    region_size = len(region)
                    input_data = region[input_offset : input_offset + input_size]
                    output_offset, output_size = process(input_data, region, region_size)
                finally:
                    region.close()

                # After answering, so that this request reports the size it was given: extend the
                # file past the server's mapping once (`--extend-to`), the way a command with a
                # writable descriptor can - only shrinking is sealed.
                extend_to = int(sys.argv[sys.argv.index("--extend-to") + 1])
                if os.fstat(fd).st_size < extend_to:
                    os.ftruncate(fd, extend_to)
            finally:
                os.close(fd)

            write_varint(stdout, request_id)
            write_varint(stdout, STATUS_OK)
            write_varint(stdout, output_offset)
            write_varint(stdout, output_size)
        except NeedMoreSpace as need_more_space:
            write_varint(stdout, request_id)
            write_varint(stdout, STATUS_NEED_MORE_SPACE)
            write_varint(stdout, need_more_space.required_size)
        except Exception as exception:  # noqa: BLE001
            write_varint(stdout, request_id)
            write_varint(stdout, STATUS_ERROR)
            write_string_binary(stdout, str(exception))

        stdout.flush()


if __name__ == "__main__":
    main()
