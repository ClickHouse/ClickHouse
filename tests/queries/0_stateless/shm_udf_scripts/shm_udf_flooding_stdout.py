#!/usr/bin/python3

# Misbehaving pooled UDF: it answers every request correctly and then floods its `stdout` with far
# more than a pipe can hold, and blocks in `write` doing it.
#
# `shm_udf_chatty.py` is the same accident one byte at a time; this is the version that also traps
# the server. Nothing reads that pipe once the response frame has been taken off it, so the child
# stays stuck in `write` forever. Closing its `stdin` - which is what discarding a worker means -
# does not help: a process blocked in `write` is not waiting for input. A server that reaps such a
# child with a plain blocking `waitpid` never returns from it, and the query hangs with its result
# already computed. The bytes have to be taken off the pipe and thrown away so the child can reach
# its own exit, and the wait has to be bounded either way.
#
# It answers with its own pid, so a test can tell a reused worker from a fresh one.

import mmap
import os
import sys

PROTOCOL_VERSION = 1
STATUS_OK = 0

# Comfortably past the 64 KiB a Linux pipe holds by default, so the write cannot finish on its own.
GARBAGE_SIZE = 4 * 1024 * 1024


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
            break
    return bytes(out)


def main():
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer

    while True:
        version = read_varint(stdin)
        if version is None:
            break  # stdin closed -> exit
        request_id = read_varint(stdin)
        if version != PROTOCOL_VERSION:
            raise RuntimeError(f"unsupported protocol version {version}")

        path_length = read_varint(stdin)
        path = stdin.read(path_length).decode("utf-8")
        input_offset = read_varint(stdin)
        input_size = read_varint(stdin)

        fd = os.open(path, os.O_RDWR)
        try:
            region = mmap.mmap(fd, 0)
        finally:
            os.close(fd)

        try:
            output = bytearray()
            for line in region[input_offset : input_offset + input_size].split(b"\n"):
                if line != b"":
                    output += str(os.getpid()).encode("ascii") + b"\n"

            output_offset = input_size
            region[output_offset : output_offset + len(output)] = bytes(output)
            region.flush()
        finally:
            region.close()

        # The response frame, and then bytes that are part of nothing at all. They go out in one
        # write so that the garbage is in the pipe by the time the server has read the frame out of
        # it: flushing the frame first would leave the server free to finish the whole invocation
        # before this process is scheduled again, and it would find two clean pipes and hand a
        # poisoned worker to the next query - the very thing being tested, decided by a race.
        # The first pipeful goes through; the rest blocks here until someone drains it.
        stdout.write(
            encode_varint(request_id)
            + encode_varint(STATUS_OK)
            + encode_varint(output_offset)
            + encode_varint(len(output))
            + b"x" * GARBAGE_SIZE
        )
        stdout.flush()

    # Reached only once the flood above has been drained and `stdin` has hit EOF. Exiting cleanly
    # is the point: the exit code is what `check_exit_code` inspects, and it must be able to.
    sys.exit(0)


if __name__ == "__main__":
    main()
