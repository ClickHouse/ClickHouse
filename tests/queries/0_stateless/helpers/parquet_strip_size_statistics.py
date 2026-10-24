#!/usr/bin/env python3
# Removes `size_statistics` (field 16 of `ColumnMetaData`) from the footer of a Parquet file, as in the files written by
# older versions of PyArrow. The footer is re-encoded in the Thrift compact protocol, the rest of the file is kept as is.
# Usage: parquet_strip_size_statistics.py <input> <output>

import struct
import sys

# Thrift compact protocol types.
BOOL_TRUE, BOOL_FALSE, BYTE, I16, I32, I64, DOUBLE, BINARY, LIST, SET, MAP, STRUCT = range(1, 13)

# The path to the field to drop: `FileMetaData.row_groups` -> `RowGroup.columns` -> `ColumnChunk.meta_data` ->
# `ColumnMetaData.size_statistics`.
DROP_PATH = {4: {1: {3: {16: None}}}}


def read_varint(buf, pos):
    result = 0
    shift = 0
    while True:
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        shift += 7
        if not byte & 0x80:
            return result, pos


def write_varint(value):
    out = bytearray()
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def skip_value(buf, pos, value_type):
    if value_type in (BOOL_TRUE, BOOL_FALSE):
        return pos
    if value_type == BYTE:
        return pos + 1
    if value_type in (I16, I32, I64):
        return read_varint(buf, pos)[1]
    if value_type == DOUBLE:
        return pos + 8
    if value_type == BINARY:
        size, pos = read_varint(buf, pos)
        return pos + size
    if value_type in (LIST, SET):
        size, element_type, pos = read_list_header(buf, pos)
        # The elements of a list of booleans take a byte each.
        if element_type in (BOOL_TRUE, BOOL_FALSE):
            element_type = BYTE
        for _ in range(size):
            pos = skip_value(buf, pos, element_type)
        return pos
    if value_type == MAP:
        size, pos = read_varint(buf, pos)
        if size:
            types = buf[pos]
            pos += 1
            for _ in range(size):
                pos = skip_value(buf, pos, types >> 4)
                pos = skip_value(buf, pos, types & 0x0F)
        return pos
    if value_type == STRUCT:
        return copy_struct(buf, pos, {}, bytearray())
    raise ValueError(f"Unknown Thrift type {value_type}")


def read_list_header(buf, pos):
    header = buf[pos]
    pos += 1
    size = header >> 4
    if size == 15:
        size, pos = read_varint(buf, pos)
    return size, header & 0x0F, pos


def copy_struct(buf, pos, drop_path, out):
    """Copies a struct from `buf` at `pos` to `out` without the fields in `drop_path`, returns the position after it."""
    last_field_in = 0
    last_field_out = 0
    while True:
        header = buf[pos]
        pos += 1
        if header == 0:
            out.append(0)
            return pos
        field_type = header & 0x0F
        delta = header >> 4
        if delta:
            field_id = last_field_in + delta
        else:
            zigzag, pos = read_varint(buf, pos)
            field_id = (zigzag >> 1) ^ -(zigzag & 1)
        last_field_in = field_id

        if field_id in drop_path and drop_path[field_id] is None:
            pos = skip_value(buf, pos, field_type)
            continue

        if 0 < field_id - last_field_out <= 15:
            out.append(((field_id - last_field_out) << 4) | field_type)
        else:
            out.append(field_type)
            out += write_varint((field_id << 1) ^ (field_id >> 63))
        last_field_out = field_id

        nested_path = drop_path.get(field_id)
        if nested_path and field_type == STRUCT:
            pos = copy_struct(buf, pos, nested_path, out)
        elif nested_path and field_type == LIST:
            start = pos
            size, _, pos = read_list_header(buf, pos)
            out += buf[start:pos]
            for _ in range(size):
                pos = copy_struct(buf, pos, nested_path, out)
        else:
            start = pos
            pos = skip_value(buf, pos, field_type)
            out += buf[start:pos]


def main():
    with open(sys.argv[1], "rb") as f:
        data = f.read()
    assert data[-4:] == b"PAR1"
    footer_size = struct.unpack("<I", data[-8:-4])[0]
    footer_start = len(data) - 8 - footer_size
    footer = bytearray()
    copy_struct(data[footer_start:-8], 0, DROP_PATH, footer)
    with open(sys.argv[2], "wb") as f:
        f.write(data[:footer_start] + bytes(footer) + struct.pack("<I", len(footer)) + b"PAR1")


if __name__ == "__main__":
    main()
