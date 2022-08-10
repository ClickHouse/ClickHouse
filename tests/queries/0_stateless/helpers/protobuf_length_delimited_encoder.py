#!/usr/bin/env python3

# The protobuf compiler protoc doesn't support encoding or decoding length-delimited protobuf message.
# To do that this script has been written.

import argparse
import os.path
import struct
import subprocess
import sys
import tempfile


def read_varint(input):
    res = 0
    multiplier = 1
    while True:
        c = input.read(1)
        if len(c) == 0:
            return None
        b = c[0]
        if b < 0x80:
            res += b * multiplier
            break
        b -= 0x80
        res += b * multiplier
        multiplier *= 0x80
    return res


def write_varint(output, value):
    while True:
        if value < 0x80:
            b = value
            output.write(b.to_bytes(1, byteorder="little"))
            break
        b = (value & 0x7F) + 0x80
        output.write(b.to_bytes(1, byteorder="little"))
        value = value >> 7


def write_hexdump(output, data):
    with subprocess.Popen(
        ["hexdump", "-C"], stdin=subprocess.PIPE, stdout=output, shell=False
    ) as proc:
        proc.communicate(data)
        if proc.returncode != 0:
            raise RuntimeError("hexdump returned code " + str(proc.returncode))
    output.flush()


class FormatSchemaSplitted:
    def __init__(self, format_schema):
        self.format_schema = format_schema
        splitted = self.format_schema.split(":")
        if len(splitted) < 2:
            raise RuntimeError(
                'The format schema must have the format "schemafile:MessageType"'
            )
        path = splitted[0]
        self.schemadir = os.path.dirname(path)
        self.schemaname = os.path.basename(path)
        if not self.schemaname.endswith(".proto"):
            self.schemaname = self.schemaname + ".proto"
        self.message_type = splitted[1]


def decode(input, output, format_schema):
    if not type(format_schema) is FormatSchemaSplitted:
        format_schema = FormatSchemaSplitted(format_schema)
    msgindex = 1
    while True:
        sz = read_varint(input)
        if sz is None:
            break
        output.write(
            "MESSAGE #{msgindex} AT 0x{msgoffset:08X}\n".format(
                msgindex=msgindex, msgoffset=input.tell()
            ).encode()
        )
        output.flush()
        msg = input.read(sz)
        if len(msg) < sz:
            raise EOFError("Unexpected end of file")
        protoc = os.getenv("PROTOC_BINARY", "protoc")
        with subprocess.Popen(
            [protoc, "--decode", format_schema.message_type, format_schema.schemaname],
            cwd=format_schema.schemadir,
            stdin=subprocess.PIPE,
            stdout=output,
            shell=False,
        ) as proc:
            proc.communicate(msg)
            if proc.returncode != 0:
                raise RuntimeError("protoc returned code " + str(proc.returncode))
        output.flush()
        msgindex = msgindex + 1


def encode(input, output, format_schema):
    if not type(format_schema) is FormatSchemaSplitted:
        format_schema = FormatSchemaSplitted(format_schema)
    line_offset = input.tell()
    line = input.readline()
    while True:
        if len(line) == 0:
            break
        if not line.startswith(b"MESSAGE #"):
            raise RuntimeError(
                "The line at 0x{line_offset:08X} must start with the text 'MESSAGE #'".format(
                    line_offset=line_offset
                )
            )
        msg = b""
        while True:
            line_offset = input.tell()
            line = input.readline()
            if line.startswith(b"MESSAGE #") or len(line) == 0:
                break
            msg += line
        protoc = os.getenv("PROTOC_BINARY", "protoc")
        with subprocess.Popen(
            [protoc, "--encode", format_schema.message_type, format_schema.schemaname],
            cwd=format_schema.schemadir,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            shell=False,
        ) as proc:
            msgbin = proc.communicate(msg)[0]
            if proc.returncode != 0:
                raise RuntimeError("protoc returned code " + str(proc.returncode))
        write_varint(output, len(msgbin))
        output.write(msgbin)
        output.flush()


def decode_and_check(input, output, format_schema):
    input_data = input.read()
    output.write(b"Binary representation:\n")
    output.flush()
    write_hexdump(output, input_data)
    output.write(b"\n")
    output.flush()

    with tempfile.TemporaryFile() as tmp_input, tempfile.TemporaryFile() as tmp_decoded, tempfile.TemporaryFile() as tmp_encoded:
        tmp_input.write(input_data)
        tmp_input.flush()
        tmp_input.seek(0)
        decode(tmp_input, tmp_decoded, format_schema)
        tmp_decoded.seek(0)
        decoded_text = tmp_decoded.read()
        output.write(decoded_text)
        output.flush()
        tmp_decoded.seek(0)
        encode(tmp_decoded, tmp_encoded, format_schema)
        tmp_encoded.seek(0)
        encoded_data = tmp_encoded.read()

    if encoded_data == input_data:
        output.write(b"\nBinary representation is as expected\n")
        output.flush()
    else:
        output.write(
            b"\nBinary representation differs from the expected one (listed below):\n"
        )
        output.flush()
        write_hexdump(output, encoded_data)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Encodes or decodes length-delimited protobuf messages."
    )
    parser.add_argument(
        "--input",
        help="The input file, the standard input will be used if not specified.",
    )
    parser.add_argument(
        "--output",
        help="The output file, the standard output will be used if not specified",
    )
    parser.add_argument(
        "--format_schema",
        required=True,
        help='Format schema in the format "schemafile:MessageType"',
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--encode",
        action="store_true",
        help="Specify to encode length-delimited messages."
        "The utility will read text-format messages of the given type from the input and write it in binary to the output.",
    )
    group.add_argument(
        "--decode",
        action="store_true",
        help="Specify to decode length-delimited messages."
        "The utility will read messages in binary from the input and write text-format messages to the output.",
    )
    group.add_argument(
        "--decode_and_check",
        action="store_true",
        help="The same as --decode, and the utility will then encode "
        " the decoded data back to the binary form to check that the result of that encoding is the same as the input was.",
    )
    args = parser.parse_args()

    custom_input_file = None
    custom_output_file = None
    try:
        if args.input:
            custom_input_file = open(args.input, "rb")
        if args.output:
            custom_output_file = open(args.output, "wb")
        input = custom_input_file if custom_input_file else sys.stdin.buffer
        output = custom_output_file if custom_output_file else sys.stdout.buffer

        if args.encode:
            encode(input, output, args.format_schema)
        elif args.decode:
            decode(input, output, args.format_schema)
        elif args.decode_and_check:
            decode_and_check(input, output, args.format_schema)

    finally:
        if custom_input_file:
            custom_input_file.close()
        if custom_output_file:
            custom_output_file.close()
