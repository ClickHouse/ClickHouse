#!/usr/bin/python3

# A pooled shared-memory UDF (see `shm_protocol.py`) that answers correctly - with `Key <row>`, right
# after the input - and does to its region's file what the flags say. A command holds a writable
# descriptor to its region, and the seals stop it only from shrinking the file: it can extend the file,
# commit pages past its end (`fallocate` with `FALLOC_FL_KEEP_SIZE`), or free pages inside it
# (`FALLOC_FL_PUNCH_HOLE`). What the server does about it is the test's business.

import argparse
import ctypes
import mmap
import os
import sys

# CI runs Python with `PYTHONSAFEPATH`, which keeps the script's own directory out of `sys.path`.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from shm_protocol import NeedMoreSpace, answer_rows, key_row, place_answer, serve  # noqa: E402

FALLOC_FL_KEEP_SIZE = 0x01
FALLOC_FL_PUNCH_HOLE = 0x02

parser = argparse.ArgumentParser()
# Extends the file before answering, on every request: to twice its size, or - with `page` - to one
# page. The pages it adds are what the server has to notice at the next hand-over. A file shorter than
# a page holds one either way; with `page` the point is whether the cap is on the pages or on the
# length.
parser.add_argument("--extend", choices=["double", "page"])
# Commits pages past the end of the file before any response goes out - the answer, or a request for a
# larger region - so that the pages are there by the time the server acts on it: twice the file's
# length of them, right past its end. The file's length does not change, so a server that measured its
# regions by their length alone would never see those pages.
#
# With values, the pages go somewhere else: three of them (or as many as fit in the number of bytes
# the second value gives - bytes, so that a configuration means the same on every page size) at the
# absolute offset the first value gives, far past the end of the file, the same ones on every call. A
# growth of the file that stops short of them commits its own pages on top of them, not instead of
# them. A third value stretches the file to that length first, without committing a page
# (`ftruncate`): length without pages, next to pages without length.
parser.add_argument("--alloc-beyond-eof", nargs="*", type=int, metavar="OFFSET BYTES LENGTH")
# After answering and before the response goes out, so that the test finds the hole there the moment
# the query returns: frees every page past the answer with `FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE`.
# The file stays exactly as long. Not the answer itself - the server is about to read it, and a command
# that wiped its own answer would just fail its own query.
parser.add_argument("--punch-hole", action="store_true")
# Like `--punch-hole`, and then commits as many pages past the end of the file with
# `FALLOC_FL_KEEP_SIZE`: the file keeps its length and its number of committed pages, so a server that
# measures the region by those two sees nothing - until something writes into the holes and the kernel
# allocates them again.
parser.add_argument("--hide-holes", action="store_true")
# While the file is shorter than the given number of bytes: frees every page of it but the first and
# commits as many pages past that many bytes - the same number of pages, so a server that measures the
# region by their count sees nothing - and asks for a region of that many bytes (`NEED_MORE_SPACE`).
# The growth commits only the pages from what the server last committed to the new length, so the
# freed pages stay free and the footprint stays within what the bound computed before it allowed.
parser.add_argument("--hide-holes-then-ask", type=int, metavar="BYTES")
# Answers every row with the size of the region's file, writes the answer at the very end of that file,
# and, once, extends the file to the given size after answering - so that this request reports the
# size it was given. The next answer then lies past what the server mapped when it created the region:
# a server that brought its mapping up to the file reads it, one that kept working on the part it had
# mapped rejects the offset.
parser.add_argument("--report-size", type=int, metavar="EXTEND_TO")
# Reports, for every input row, whether the probed stretch of the region still holds anything: `clean`
# if the 4 KiB right after the input (or at `--peek-at`) are all zero, `dirty` otherwise. A previous
# request's larger input or output leaves its bytes there unless the server scrubbed them; this is how
# the test sees whether it did. Then overwrites that stretch, so that the next request finds it dirty
# unless scrubbed again.
parser.add_argument("--peek", action="store_true")
parser.add_argument("--peek-at", type=int, metavar="OFFSET")
# Extends the file to the given size once, before answering: the tail the server never mapped is where
# a scrub that only covered the mapping would miss.
parser.add_argument("--extend-to", type=int, metavar="BYTES")
args = parser.parse_args()


def fallocate(fd, mode, offset, length, mode_name):
    libc = ctypes.CDLL(None, use_errno=True)
    libc.fallocate.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_int64, ctypes.c_int64]
    if libc.fallocate(fd, mode, offset, length) != 0:
        raise OSError(ctypes.get_errno(), f"fallocate({mode_name}) failed")


def extend_to(fd, length):
    if os.fstat(fd).st_size < length:
        os.ftruncate(fd, length)


def allocate_beyond_eof(fd):
    far_offset, far_bytes, sparse_length = (args.alloc_beyond_eof + [None] * 3)[:3]
    if sparse_length is not None:
        extend_to(fd, sparse_length)
    if far_offset is None:
        size = os.fstat(fd).st_size
        offset, length = size, max(2 * size, 3 * mmap.PAGESIZE)
    else:
        offset = far_offset
        length = 3 * mmap.PAGESIZE if far_bytes is None else far_bytes // mmap.PAGESIZE * mmap.PAGESIZE
    fallocate(fd, FALLOC_FL_KEEP_SIZE, offset, length, "FALLOC_FL_KEEP_SIZE")


def before_mapping(fd):
    if args.hide_holes_then_ask is not None:
        size = os.fstat(fd).st_size
        if size < args.hide_holes_then_ask:
            page = mmap.PAGESIZE
            freed = size - page
            fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, page, freed, "FALLOC_FL_PUNCH_HOLE")
            # Past the length asked for, so that the growth does not commit them in its own range.
            fallocate(fd, FALLOC_FL_KEEP_SIZE, args.hide_holes_then_ask, freed, "FALLOC_FL_KEEP_SIZE")
            raise NeedMoreSpace(args.hide_holes_then_ask)
    if args.extend == "double":
        os.ftruncate(fd, os.fstat(fd).st_size * 2)
    elif args.extend == "page":
        os.ftruncate(fd, mmap.PAGESIZE)
    if args.alloc_beyond_eof is not None:
        allocate_beyond_eof(fd)
    if args.extend_to is not None:
        extend_to(fd, args.extend_to)


def after_answer(fd, output_offset, output_size):
    if args.report_size is not None:
        extend_to(fd, args.report_size)

    if args.punch_hole or args.hide_holes:
        # Every page from the end of the answer (rounded up to a page) to the end of the file.
        page = mmap.PAGESIZE
        offset = (output_offset + output_size + page - 1) // page * page
        size = os.fstat(fd).st_size
        if offset >= size:
            return
        fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, size - offset, "FALLOC_FL_PUNCH_HOLE")
        if args.hide_holes:
            fallocate(fd, FALLOC_FL_KEEP_SIZE, size, size - offset, "FALLOC_FL_KEEP_SIZE")


def report_size(input_data, region):
    output = str(len(region)).encode() + b"\n"
    output *= sum(1 for row in input_data.split(b"\n") if row != b"")
    # At the very end of the file rather than right after the input - but only in a region that holds
    # the input and the answer next to each other.
    if len(input_data) + len(output) > len(region):
        raise NeedMoreSpace(len(input_data) + len(output))
    return place_answer(region, len(region) - len(output), output)


def peek(input_data, region):
    probe_from = args.peek_at if args.peek_at is not None else len(input_data)
    probe = bytes(region[probe_from : probe_from + 4096])
    verdict = b"clean" if probe.strip(b"\x00") == b"" else b"dirty"
    region[probe_from : probe_from + 4096] = b"x" * 4096
    return answer_rows(lambda row: verdict + b"\n")(input_data, region)


def main():
    if args.report_size is not None:
        answer = report_size
    elif args.peek:
        answer = peek
    else:
        answer = answer_rows(key_row)
    serve(answer, before_mapping, after_answer)


if __name__ == "__main__":
    main()
