# -*- coding: utf-8 -*-
import uuid

from testflows._core.testtype import TestSubType
from testflows.core.name import basename, parentname
from testflows.core import current

modes = [
    # mode, key_len, iv_len, aad
    ("'aes-128-ecb'", 16, None, None),
    ("'aes-192-ecb'", 24, None, None),
    ("'aes-256-ecb'", 32, None, None),
    # cbc
    ("'aes-128-cbc'", 16, None, None),
    ("'aes-192-cbc'", 24, None, None),
    ("'aes-256-cbc'", 32, None, None),
    ("'aes-128-cbc'", 16, 16, None),
    ("'aes-192-cbc'", 24, 16, None),
    ("'aes-256-cbc'", 32, 16, None),
    # cfb128
    ("'aes-128-cfb128'", 16, None, None),
    ("'aes-192-cfb128'", 24, None, None),
    ("'aes-256-cfb128'", 32, None, None),
    ("'aes-128-cfb128'", 16, 16, None),
    ("'aes-192-cfb128'", 24, 16, None),
    ("'aes-256-cfb128'", 32, 16, None),
    # ofb
    ("'aes-128-ofb'", 16, None, None),
    ("'aes-192-ofb'", 24, None, None),
    ("'aes-256-ofb'", 32, None, None),
    ("'aes-128-ofb'", 16, 16, None),
    ("'aes-192-ofb'", 24, 16, None),
    ("'aes-256-ofb'", 32, 16, None),
    # gcm
    ("'aes-128-gcm'", 16, 12, None),
    ("'aes-192-gcm'", 24, 12, None),
    ("'aes-256-gcm'", 32, 12, None),
    ("'aes-128-gcm'", 16, 12, True),
    ("'aes-192-gcm'", 24, 12, True),
    ("'aes-256-gcm'", 32, 12, True),
    # ctr
    ("'aes-128-ctr'", 16, None, None),
    ("'aes-192-ctr'", 24, None, None),
    ("'aes-256-ctr'", 32, None, None),
    ("'aes-128-ctr'", 16, 16, None),
    ("'aes-192-ctr'", 24, 16, None),
    ("'aes-256-ctr'", 32, 16, None),
]

mysql_modes = [
    # mode, key_len, iv_len
    ("'aes-128-ecb'", 16, None),
    ("'aes-128-ecb'", 24, None),
    ("'aes-192-ecb'", 24, None),
    ("'aes-192-ecb'", 32, None),
    ("'aes-256-ecb'", 32, None),
    ("'aes-256-ecb'", 64, None),
    # cbc
    ("'aes-128-cbc'", 16, None),
    ("'aes-192-cbc'", 24, None),
    ("'aes-256-cbc'", 32, None),
    ("'aes-128-cbc'", 16, 16),
    ("'aes-128-cbc'", 24, 24),
    ("'aes-192-cbc'", 24, 16),
    ("'aes-192-cbc'", 32, 32),
    ("'aes-256-cbc'", 32, 16),
    ("'aes-256-cbc'", 64, 64),
    # cfb128
    ("'aes-128-cfb128'", 16, None),
    ("'aes-192-cfb128'", 24, None),
    ("'aes-256-cfb128'", 32, None),
    ("'aes-128-cfb128'", 16, 16),
    ("'aes-128-cfb128'", 24, 24),
    ("'aes-192-cfb128'", 24, 16),
    ("'aes-192-cfb128'", 32, 32),
    ("'aes-256-cfb128'", 32, 16),
    ("'aes-256-cfb128'", 64, 64),
    # ofb
    ("'aes-128-ofb'", 16, None),
    ("'aes-192-ofb'", 24, None),
    ("'aes-256-ofb'", 32, None),
    ("'aes-128-ofb'", 16, 16),
    ("'aes-128-ofb'", 24, 24),
    ("'aes-192-ofb'", 24, 16),
    ("'aes-192-ofb'", 32, 32),
    ("'aes-256-ofb'", 32, 16),
    ("'aes-256-ofb'", 64, 64),
]

plaintexts = [
    ("bytes", "unhex('0')"),
    ("emptystring", "''"),
    ("utf8string", "'Gãńdåłf_Thê_Gręât'"),
    ("utf8fixedstring", "toFixedString('Gãńdåłf_Thê_Gręât', 24)"),
    ("String", "'1'"),
    ("FixedString", "toFixedString('1', 1)"),
    ("UInt8", "reinterpretAsFixedString(toUInt8('1'))"),
    ("UInt16", "reinterpretAsFixedString(toUInt16('1'))"),
    ("UInt32", "reinterpretAsFixedString(toUInt32('1'))"),
    ("UInt64", "reinterpretAsFixedString(toUInt64('1'))"),
    ("Int8", "reinterpretAsFixedString(toInt8('1'))"),
    ("Int16", "reinterpretAsFixedString(toInt16('1'))"),
    ("Int32", "reinterpretAsFixedString(toInt32('1'))"),
    ("Int64", "reinterpretAsFixedString(toInt64('1'))"),
    ("Float32", "reinterpretAsFixedString(toFloat32('1'))"),
    ("Float64", "reinterpretAsFixedString(toFloat64('1'))"),
    ("Decimal32", "reinterpretAsFixedString(toDecimal32(2, 4))"),
    ("Decimal64", "reinterpretAsFixedString(toDecimal64(2, 4))"),
    ("Decimal128", "reinterpretAsFixedString(toDecimal128(2, 4))"),
    ("UUID", "reinterpretAsFixedString(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'))"),
    ("Date", "reinterpretAsFixedString(toDate('2020-01-01'))"),
    ("DateTime", "reinterpretAsFixedString(toDateTime('2020-01-01 20:01:02'))"),
    ("DateTime64", "reinterpretAsFixedString(toDateTime64('2020-01-01 20:01:02.123', 3))"),
    ("LowCardinality", "toLowCardinality('1')"),
    ("LowCardinalityFixedString", "toLowCardinality(toFixedString('1',2))"),
    #("Array", "[1,2]"), - not supported
    #("Tuple", "(1,'a')") - not supported
    ("NULL", "reinterpretAsFixedString(toDateOrNull('foo'))"),
    ("NullableString", "toNullable('1')"),
    ("NullableStringNull", "toNullable(NULL)"),
    ("NullableFixedString", "toNullable(toFixedString('1',2))"),
    ("NullableFixedStringNull", "toNullable(toFixedString(NULL,2))"),
    ("IPv4", "reinterpretAsFixedString(toIPv4('171.225.130.45'))"),
    ("IPv6", "reinterpretAsFixedString(toIPv6('2001:0db8:0000:85a3:0000:0000:ac1f:8001'))"),
    ("Enum8", r"reinterpretAsFixedString(CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2)'))"),
    ("Enum16", r"reinterpretAsFixedString(CAST('a', 'Enum16(\'a\' = 1, \'b\' = 2)'))"),
]

_hex = hex

def hex(s):
    """Convert string to hex.
    """
    if isinstance(s, str):
        return "".join(['%X' % ord(c) for c in s])
    if isinstance(s, bytes):
        return "".join(['%X' % c for c in s])
    return _hex(s)

def getuid():
    if current().subtype == TestSubType.Example:
        testname = f"{basename(parentname(current().name)).replace(' ', '_').replace(',','')}"
    else:
        testname = f"{basename(current().name).replace(' ', '_').replace(',','')}"
    return testname + "_" + str(uuid.uuid1()).replace('-', '_')
