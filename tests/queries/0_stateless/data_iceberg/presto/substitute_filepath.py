#!/usr/bin/env python3
"""Copy a Presto golden Iceberg table and substitute its `FILEPATH` placeholder.

Usage: substitute_filepath.py <source table directory> <destination table directory>

The tables in `presto-iceberg/src/test/resources/iceberg_v3` and
`presto-native-tests/src/test/resources/iceberg_v3` store every path with the placeholder
`FILEPATH` in place of the table location. Presto's tests
(`IcebergImportedTableTestBase.setupAndRegisterTable`) copy the table into a temporary
directory and replace `FILEPATH` with the absolute path of the copy in every `.json` and
`.avro` file of `metadata/`. This script does the same. Unlike Presto, which re-encodes the
Avro files with `DataFileWriter` and so drops the Avro header metadata (`partition-spec`,
`schema`, `format-version`, ...) and the codec, it keeps the header and the codec of every
Avro file as they are and re-encodes only the records, with the substituted strings.
"""

import json
import os
import shutil
import sys
import zlib

PLACEHOLDER = "FILEPATH"
MAGIC = b"Obj\x01"


class Reader:
    def __init__(self, data, pos=0):
        self.data = data
        self.pos = pos

    def read(self, n):
        if self.pos + n > len(self.data):
            raise ValueError("unexpected end of Avro data")
        b = self.data[self.pos : self.pos + n]
        self.pos += n
        return b

    def long(self):
        shift = 0
        n = 0
        while True:
            c = self.read(1)[0]
            n |= (c & 0x7F) << shift
            shift += 7
            if c < 0x80:
                return (n >> 1) ^ -(n & 1)


def encode_long(n):
    n = (n << 1) ^ (n >> 63)
    out = bytearray()
    while n > 0x7F:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n)
    return bytes(out)


class Transcoder:
    """Copies Avro-encoded values of a schema, applying `replace` to every string."""

    def __init__(self, schema, replace):
        self.named = {}
        self.replace = replace
        self.schema = schema
        self.register(schema, None)

    def register(self, schema, namespace):
        if isinstance(schema, list):
            for s in schema:
                self.register(s, namespace)
        elif isinstance(schema, dict):
            t = schema["type"]
            if t in ("record", "enum", "fixed", "error"):
                name = schema["name"]
                namespace = schema.get("namespace", namespace)
                if "." not in name and namespace:
                    name = namespace + "." + name
                self.named[name] = schema
                self.named[schema["name"].split(".")[-1]] = schema
                for f in schema.get("fields", []):
                    self.register(f["type"], namespace)
            elif t == "array":
                self.register(schema["items"], namespace)
            elif t == "map":
                self.register(schema["values"], namespace)
            else:
                self.register(t, namespace)

    def resolve(self, schema):
        if isinstance(schema, str) and schema in self.named:
            return self.named[schema]
        if isinstance(schema, str) and schema.split(".")[-1] in self.named:
            return self.named[schema.split(".")[-1]]
        return schema

    def copy(self, schema, r, out):
        schema = self.resolve(schema)
        if isinstance(schema, list):
            index = r.long()
            out += encode_long(index)
            self.copy(schema[index], r, out)
            return
        t = schema["type"] if isinstance(schema, dict) else schema
        if isinstance(t, (dict, list)):
            self.copy(t, r, out)
        elif t == "null":
            pass
        elif t == "boolean":
            out += r.read(1)
        elif t in ("int", "long", "enum"):
            out += encode_long(r.long())
        elif t == "float":
            out += r.read(4)
        elif t == "double":
            out += r.read(8)
        elif t == "fixed":
            out += r.read(schema["size"])
        elif t == "bytes":
            out += self.blob(r)
        elif t == "string":
            s = self.replace(r.read(r.long()).decode("utf-8")).encode("utf-8")
            out += encode_long(len(s)) + s
        elif t == "record" or t == "error":
            for f in schema["fields"]:
                self.copy(f["type"], r, out)
        elif t in ("array", "map"):
            while True:
                count = r.long()
                if count < 0:
                    count = -count
                    r.long()  # The byte size of the block, which changes, so it is not written.
                out += encode_long(count)
                if count == 0:
                    break
                for _ in range(count):
                    if t == "map":
                        out += self.blob(r, replace=True)
                        self.copy(schema["values"], r, out)
                    else:
                        self.copy(schema["items"], r, out)
        elif t in self.named:
            self.copy(self.named[t], r, out)
        else:
            raise ValueError("unsupported Avro type: " + repr(t))

    def blob(self, r, replace=False):
        b = r.read(r.long())
        if replace:
            b = self.replace(b.decode("utf-8")).encode("utf-8")
        return encode_long(len(b)) + b


def substitute_avro(data, replace):
    r = Reader(data)
    if r.read(4) != MAGIC:
        raise ValueError("not an Avro object container file")
    meta = {}
    while True:
        count = r.long()
        if count == 0:
            break
        if count < 0:
            count = -count
            r.long()
        for _ in range(count):
            key = r.read(r.long()).decode("utf-8")
            meta[key] = r.read(r.long())
    sync = r.read(16)
    header = data[: r.pos]
    if PLACEHOLDER.encode() in header:
        raise ValueError("the placeholder in the Avro header is not supported")
    codec = meta.get("avro.codec", b"null").decode()
    if codec not in ("null", "deflate"):
        raise ValueError("unsupported Avro codec: " + codec)
    transcoder = Transcoder(json.loads(meta["avro.schema"]), replace)
    out = bytearray(header)
    while r.pos < len(data):
        count = r.long()
        block = r.read(r.long())
        if codec == "deflate":
            block = zlib.decompress(block, -15)
        br = Reader(block)
        records = bytearray()
        for _ in range(count):
            transcoder.copy(transcoder.schema, br, records)
        if br.pos != len(block):
            raise ValueError("trailing bytes in an Avro block")
        if codec == "deflate":
            c = zlib.compressobj(9, zlib.DEFLATED, -15)
            records = c.compress(bytes(records)) + c.flush()
        if r.read(16) != sync:
            raise ValueError("Avro sync marker mismatch")
        out += encode_long(count) + encode_long(len(records)) + records + sync
    return bytes(out)


def main():
    source, destination = sys.argv[1], os.path.abspath(sys.argv[2])
    shutil.copytree(source, destination)

    def replace(s):
        return s.replace(PLACEHOLDER, destination)

    metadata = os.path.join(destination, "metadata")
    for name in sorted(os.listdir(metadata)):
        path = os.path.join(metadata, name)
        with open(path, "rb") as f:
            data = f.read()
        if name.endswith(".avro"):
            data = substitute_avro(data, replace)
        elif name.endswith(".json"):
            data = replace(data.decode("utf-8")).encode("utf-8")
        else:
            continue
        with open(path, "wb") as f:
            f.write(data)


if __name__ == "__main__":
    main()
