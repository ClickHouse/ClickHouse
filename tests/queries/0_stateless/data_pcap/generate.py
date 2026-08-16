#!/usr/bin/env python3
"""Generate self-authored PCAP fixtures for the `PCAP` format test."""

import struct
from pathlib import Path


def ipv4(protocol, payload):
    total_length = 20 + len(payload)
    return bytes.fromhex("4500") + struct.pack(">H", total_length) + bytes.fromhex("0000400040") + bytes([protocol]) + bytes.fromhex("0000c0000201c0000202") + payload


def ethernet(payload):
    return bytes.fromhex("00112233445566778899aabb0800") + payload


tcp = struct.pack(">HHIIBBHHH", 12345, 80, 1, 0, 0x50, 0x18, 1024, 0, 0) + b"GET / HTTP/1.0\\r\\n\\r\\n"
udp = struct.pack(">HHHH", 68, 67, 8, 0)
icmp = bytes.fromhex("0800000000010001")
packets = [ethernet(ipv4(6, tcp)), ethernet(ipv4(17, udp)), ethernet(ipv4(1, icmp))]

with Path(__file__).with_name("packets.pcap").open("wb") as file:
    file.write(struct.pack("<IHHIIII", 0xA1B2C3D4, 2, 4, 0, 0, 65535, 1))
    for number, packet in enumerate(packets):
        file.write(struct.pack("<IIII", 1_700_000_000 + number, 0, len(packet), len(packet)))
        file.write(packet)
