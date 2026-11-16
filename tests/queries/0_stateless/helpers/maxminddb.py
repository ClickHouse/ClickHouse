#!/usr/bin/env python3
"""Deterministic, small MMDB fixtures without an additional test dependency."""

import ipaddress
import pathlib
import struct
import sys


class Value:
    def __init__(self, kind, value):
        self.kind = kind
        self.value = value


def header(kind, size):
    if size < 29:
        encoded_size, extra = size, b""
    elif size < 285:
        encoded_size, extra = 29, bytes([size - 29])
    else:
        encoded_size, extra = 30, (size - 285).to_bytes(2, "big")
    return (
        bytes([(kind << 5 if kind < 8 else 0) | encoded_size])
        + (bytes([kind - 7]) if kind >= 8 else b"")
        + extra
    )


def encode(value):
    if isinstance(value, Value):
        kind, value = value.kind, value.value
        if kind in (3, 15):
            data = struct.pack(">d" if kind == 3 else ">f", value)
        elif kind == 8:
            data = value.to_bytes(4, "big", signed=True)
        else:
            data = value.to_bytes(max(1, (value.bit_length() + 7) // 8), "big")
    elif isinstance(value, bool):
        return header(14, int(value))
    elif isinstance(value, str):
        kind, data = 2, value.encode()
    elif isinstance(value, bytes):
        kind, data = 4, value
    elif isinstance(value, list):
        return header(11, len(value)) + b"".join(map(encode, value))
    elif isinstance(value, dict):
        return header(7, len(value)) + b"".join(
            encode(key) + encode(item) for key, item in value.items()
        )
    else:
        raise TypeError(type(value))
    return header(kind, len(data)) + data


def write_database(path, ip_version, records):
    root = [None, None]
    for network, record in sorted(
        records, key=lambda item: ipaddress.ip_network(item[0]).prefixlen
    ):
        network = ipaddress.ip_network(network)
        node = root
        bits = network.max_prefixlen
        for position in range(network.prefixlen):
            side = (int(network.network_address) >> (bits - position - 1)) & 1
            if position == network.prefixlen - 1:
                node[side] = record
            else:
                if not isinstance(node[side], list):
                    node[side] = [node[side], node[side]]
                node = node[side]

    nodes = []
    indices = {}

    def visit(node):
        indices[id(node)] = len(nodes)
        nodes.append(node)
        for child in node:
            if isinstance(child, list):
                visit(child)

    visit(root)
    data = bytearray()
    offsets = {}
    tree = bytearray()
    for node in nodes:
        for child in node:
            if isinstance(child, list):
                pointer = indices[id(child)]
            elif child is None:
                pointer = len(nodes)
            else:
                payload = encode(child)
                if payload not in offsets:
                    offsets[payload] = len(data)
                    data.extend(payload)
                pointer = len(nodes) + 16 + offsets[payload]
            tree.extend(pointer.to_bytes(3, "big"))

    metadata = {
        "node_count": Value(6, len(nodes)),
        "record_size": Value(5, 24),
        "ip_version": Value(5, ip_version),
        "database_type": "ClickHouse-MaxMindDB-Test",
        "languages": ["en", "fr"],
        "binary_format_major_version": Value(5, 2),
        "binary_format_minor_version": Value(5, 0),
        "build_epoch": Value(9, 1790899200),
        "description": {"en": "ClickHouse functional test"},
    }
    path.write_bytes(
        tree + bytes(16) + data + b"\xab\xcd\xefMaxMind.com" + encode(metadata)
    )


def fixtures(directory):
    directory.mkdir(parents=True, exist_ok=True)
    for version in ("A", "B"):
        first = {
            "version": version,
            "country": {"iso_code": "FR", "names": {"en": "France", "fr": "France"}},
            "location": {"latitude": Value(3, 48.5), "longitude": Value(3, 2.25)},
            "array": [Value(5, 1), Value(6, 70000)],
            "nested": [{"value": Value(8, -7)}],
            "optional": {"label": "present"},
            "widened": Value(8, -2),
            "wide_mixed": Value(8, -3),
            "bool": True,
            "bytes": b"\x00\xff",
            "float": Value(15, 1.5),
            "uint16": Value(5, 42),
            "uint32": Value(6, 70000),
            "uint64": Value(9, 2**60),
            "uint128": Value(10, 2**120 + 17),
            "int32": Value(8, -123),
            "promoted": Value(5, 20),
            "empty": {},
        }
        if version == "B":
            first["added"] = "new field"
        second = dict(first)
        second.update(
            {
                "country": {"names": {"en": "Australia"}},
                "location": {"latitude": Value(3, -33.5)},
                "array": [],
                "nested": [],
                "widened": Value(6, 4000000000),
                "wide_mixed": Value(9, 2**63 + 1),
                "bool": False,
                "promoted": Value(3, 1.5),
            }
        )
        del second["optional"]
        networks = [
            ("8.8.0.0/16", second),
            ("8.8.8.0/24", first),
            ("1.1.1.0/24", second),
        ]
        write_database(directory / f"v4_{version}.mmdb", 4, networks)
        v6_networks = [("2001:db8::/32", first), ("2001:db8:1::/48", second)]
        for prefix in ("::", "::ffff:"):
            for network, record in networks:
                ipv4 = ipaddress.ip_network(network)
                mapped = int(ipaddress.ip_address(prefix + str(ipv4.network_address)))
                v6_networks.append(
                    (
                        str(ipaddress.IPv6Address(mapped)) + f"/{96 + ipv4.prefixlen}",
                        record,
                    )
                )
        write_database(directory / f"v6_{version}.mmdb", 6, v6_networks)
    invalid = dict(first, ip="reserved")
    write_database(directory / "reserved.mmdb", 4, [("8.8.8.0/24", invalid)])
    incompatible = dict(first, country="incompatible")
    write_database(directory / "incompatible.mmdb", 4, [("8.8.8.0/24", incompatible)])
    write_database(
        directory / "incompatible_inference.mmdb",
        4,
        [("8.8.8.0/24", first), ("1.1.1.0/24", incompatible)],
    )
    (directory / "invalid.mmdb").write_text("This is not an MMDB file.\n")


if __name__ == "__main__":
    fixtures(pathlib.Path(sys.argv[1]))
