#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: ORC is not built in fasttest.
# The `content_length` in the ORC footer covers only the stripes, not the metadata section with the stripe statistics.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

check()
{
    $CLICKHOUSE_LOCAL --query "$1" | python3 -c '
import sys

data = sys.stdin.buffer.read()

def varint(buf, pos):
    result = shift = 0
    while True:
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7f) << shift
        shift += 7
        if byte < 0x80:
            return result, pos

def fields(buf):
    pos = 0
    while pos < len(buf):
        key, pos = varint(buf, pos)
        if key & 7 == 0:
            value, pos = varint(buf, pos)
        elif key & 7 == 2:
            length, pos = varint(buf, pos)
            value = buf[pos:pos + length]
            pos += length
        else:
            raise Exception("unexpected wire type")
        yield key >> 3, value

postscript_length = data[-1]
postscript = dict(fields(data[-1 - postscript_length:-1]))
footer_length = postscript[1]
footer = list(fields(data[-1 - postscript_length - footer_length:-1 - postscript_length]))
content_length = next(v for f, v in footer if f == 2)
stripes = [dict(fields(v)) for f, v in footer if f == 3]
stripes_length = sum(s.get(2, 0) + s.get(3, 0) + s.get(4, 0) for s in stripes)
print(len(stripes), content_length == stripes_length, postscript[5] > 0)
'
}

check "SELECT number FROM numbers(3) FORMAT ORC SETTINGS output_format_orc_compression_method = 'none'"
check "SELECT number, toString(number) FROM numbers(100000) FORMAT ORC SETTINGS output_format_orc_compression_method = 'none'"
