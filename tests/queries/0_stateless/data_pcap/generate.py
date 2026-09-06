#!/usr/bin/env python3
"""Generate self-authored PCAP fixtures for the `PCAP` format test.

Usage: generate.py <output directory>
"""

import struct
import sys
from pathlib import Path


def ipv4(protocol, payload, flags_and_offset=0x4000):
    """`flags_and_offset` is the raw IPv4 flags/fragment-offset field; 0x4000 is "Don't Fragment"."""
    total_length = 20 + len(payload)
    return (
        bytes.fromhex("4500")
        + struct.pack(">HHH", total_length, 0, flags_and_offset)
        + bytes.fromhex("40")
        + bytes([protocol])
        + bytes.fromhex("0000c0000201c0000202")
        + payload
    )


def ipv6(next_header, payload):
    """IPv6 header (2001:db8::1 -> 2001:db8::2, hop limit 64) followed by `payload`."""
    return (
        bytes.fromhex("60000000")
        + struct.pack(">HBB", len(payload), next_header, 64)
        + bytes.fromhex("20010db8000000000000000000000001")
        + bytes.fromhex("20010db8000000000000000000000002")
        + payload
    )


def ethernet(payload, vlan_id=None, ether_type=0x0800):
    """Ethernet II carrying IPv4 (or IPv6 with `ether_type` 0x86DD), optionally behind a single 802.1Q tag."""
    frame = bytes.fromhex("00112233445566778899aabb")
    if vlan_id is not None:
        frame += struct.pack(">HH", 0x8100, vlan_id)
    return frame + struct.pack(">H", ether_type) + payload


tcp = struct.pack(">HHIIBBHHH", 12345, 80, 1, 0, 0x50, 0x18, 1024, 0, 0) + b"GET / HTTP/1.0\\r\\n\\r\\n"
udp = struct.pack(">HHHH", 68, 67, 8, 0)
icmp = bytes.fromhex("0800000000010001")
packets = [ethernet(ipv4(6, tcp)), ethernet(ipv4(17, udp)), ethernet(ipv4(1, icmp))]

# A VLAN-tagged TCP packet: `eth_type` must report the encapsulated protocol
# (`IP`) and not the 802.1Q tag, which is reported by `vlan_id` instead.
packets.append(ethernet(ipv4(6, tcp), vlan_id=42))

# A non-first IPv4 fragment (fragment offset 185, no "more fragments"): the TCP
# header is in another fragment, so the transport layer cannot be decoded, but
# `ip_protocol` still comes from the IPv4 header and reports `TCP`.
packets.append(ethernet(ipv4(6, b"a fragment of a TCP segment", flags_and_offset=185)))

# Native IPv6: a TCP and a UDP packet, so that the IPv6 branch (addresses taken
# from the IPv6 header, `ip_protocol` from the `next header` field, ports from
# the transport layer) is exercised too.
packets.append(ethernet(ipv6(6, tcp), ether_type=0x86DD))
packets.append(ethernet(ipv6(17, udp), ether_type=0x86DD))


def write_pcap(path, records, snaplen=65535):
    """Classic pcap; every record is a (captured bytes, original wire length) pair."""
    with path.open("wb") as file:
        file.write(struct.pack("<IHHIIII", 0xA1B2C3D4, 2, 4, 0, 0, snaplen, 1))
        for number, (packet, wire_length) in enumerate(records):
            file.write(struct.pack("<IIII", 1_700_000_000 + number, 0, len(packet), wire_length))
            file.write(packet)


def write_pcapng(path, records, snaplen=65535):
    """pcapng with a single section, a single Ethernet interface, and Enhanced Packet Blocks."""
    def block(block_type, body):
        total_length = len(body) + 12
        return struct.pack("<II", block_type, total_length) + body + struct.pack("<I", total_length)

    out = block(0x0A0D0D0A, struct.pack("<IHHq", 0x1A2B3C4D, 1, 0, -1))  # Section Header Block
    out += block(0x00000001, struct.pack("<HHI", 1, 0, snaplen))  # Interface Description Block, LINKTYPE_ETHERNET
    for number, (packet, wire_length) in enumerate(records):
        timestamp = (1_700_000_000 + number) * 1_000_000  # microseconds, the default resolution
        padding = b"\x00" * (-len(packet) % 4)
        out += block(0x00000006, struct.pack("<IIIII", 0, timestamp >> 32, timestamp & 0xFFFFFFFF, len(packet), wire_length) + packet + padding)
    path.write_bytes(out)


out_dir = Path(sys.argv[1])
full = [(packet, len(packet)) for packet in packets]
write_pcap(out_dir / "packets.pcap", full)
write_pcapng(out_dir / "packets.pcapng", full)

# Snapshot length of 34 bytes: Ethernet plus the IPv4 header only, so the
# captured bytes are shorter than the original wire length. Only the untagged,
# unfragmented packets are used, so that 34 bytes cut exactly after the IPv4
# header.
SNAPLEN = 34
truncated = [(packet[:SNAPLEN], len(packet)) for packet in packets[:3]]
write_pcap(out_dir / "truncated.pcap", truncated, snaplen=SNAPLEN)
