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


def ethernet(payload, vlan_id=None, ether_type=0x0800, addresses="00112233445566778899aabb"):
    """Ethernet II carrying IPv4 (or IPv6 with `ether_type` 0x86DD), optionally behind a single 802.1Q tag.

    `addresses` is the destination then the source MAC address, as hex."""
    frame = bytes.fromhex(addresses)
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


def ipv6_extension_header(header_type, next_header, body):
    """A generic IPv6 extension header (`next header`, length in 8-byte units not counting the first 8, body)."""
    assert (2 + len(body)) % 8 == 0
    return bytes([next_header, (2 + len(body)) // 8 - 1]) + body


# IPv6 with an extension-header chain: Hop-by-Hop options (0) then Routing (43)
# before TCP. `ip_protocol` must report the protocol after the chain (`TCP`),
# not the first `next header` value of the fixed header (`HOPOPT`).
hop_by_hop_then_routing = ipv6_extension_header(
    0, 43, bytes.fromhex("010400000000")  # PadN option covering the 6 remaining bytes
) + ipv6_extension_header(
    43, 6, bytes.fromhex("020000000000")  # routing type 2, no segments left
)
packets.append(ethernet(ipv6(0, hop_by_hop_then_routing + tcp), ether_type=0x86DD))

# A non-first IPv6 fragment (Fragment header 44, offset 185, no "more fragments")
# of a UDP datagram: the transport header is in another fragment, so the ports
# cannot be decoded, but the Fragment header still names the protocol (`UDP`).
fragment = bytes([17, 0]) + struct.pack(">HI", 185 << 3, 0x12345678)
packets.append(ethernet(ipv6(44, fragment + b"a fragment of a UDP datagram"), ether_type=0x86DD))

# IPv6 with an Authentication Header (51) before TCP. The AH length field is in
# 4-byte units minus 2 (RFC 4302), unlike the generic 8-byte units, so a reader
# that uses the generic rule looks for the TCP header at the wrong offset.
authentication_header = bytes([6, 4]) + bytes(2) + struct.pack(">II", 0x1000, 1) + bytes(12)  # 24 bytes, 96-bit ICV
packets.append(ethernet(ipv6(51, authentication_header + tcp), ether_type=0x86DD))

# IPv6 with a Shim6 header (140, generic extension-header format per RFC 7045)
# before UDP: the ports come from the UDP header after it.
packets.append(ethernet(ipv6(140, ipv6_extension_header(140, 17, bytes(6)) + udp), ether_type=0x86DD))


def write_pcap(path, records, snaplen=65535, linktype=1):
    """Classic pcap; every record is a (captured bytes, original wire length) pair.

    `linktype` is the link type of the capture, `LINKTYPE_ETHERNET` by default."""
    with path.open("wb") as file:
        file.write(struct.pack("<IHHIIII", 0xA1B2C3D4, 2, 4, 0, 0, snaplen, linktype))
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

# Packets cut by the snapshot length inside a header: the IPv6 packet with the
# Hop-by-Hop and Routing chain before TCP, cut inside the Hop-by-Hop header,
# inside the Routing header (after its `next header` byte), and inside the TCP
# header, and an IPv4 TCP packet cut inside the TCP header. The packets are
# dissected up to the last header captured completely, and for IPv6
# `ip_protocol` is the extension header that was cut or, after a complete
# chain, `TCP`.
chain = ethernet(ipv6(0, hop_by_hop_then_routing + tcp), ether_type=0x86DD)
chain_start = 14 + 40
truncated_headers = [(chain[:chain_start + cut], len(chain)) for cut in (4, 8 + 4, 16 + 4)]
truncated_headers.append((packets[0][:14 + 20 + 4], len(packets[0])))
write_pcap(out_dir / "truncated_headers.pcap", truncated_headers, snaplen=chain_start + 16 + 4)

# Packets cut by the snapshot length before the end of the IP header: Ethernet
# frames cut inside the IPv4 header, inside the IPv6 header, inside a VLAN tag,
# and inside the IPv4 header after a complete VLAN tag, an Ethernet frame with
# only its header captured, and raw IP packets cut inside the IPv4 and the IPv6
# header. Ethernet frames are dissected up to their last complete header (the
# Ethernet header or the VLAN tag) and the rest is raw, but `eth_type` still
# names the protocol from the type field, and `link_type` of a raw IP packet
# is still `IP` or `IPv6`.
vlan_frame = ethernet(ipv4(6, tcp), vlan_id=7)
truncated_ip = [
    (packets[0][:18], len(packets[0])),
    (chain[:14 + 20], len(chain)),
    (vlan_frame[:16], len(vlan_frame)),
    (vlan_frame[:18 + 10], len(vlan_frame)),
    (packets[0][:14], len(packets[0])),
]
write_pcap(out_dir / "truncated_ip.pcap", truncated_ip, snaplen=34)
truncated_raw_ip = [(ipv4(6, tcp)[:10], len(ipv4(6, tcp))), (ipv6(17, udp)[:20], len(ipv6(17, udp)))]
write_pcap(out_dir / "truncated_raw_ip.pcap", truncated_raw_ip, snaplen=20, linktype=101)

# A short Ethernet frame: UDP with a 4-byte payload is 46 bytes, so the sender
# pads it to the 60-byte minimum. The padding is past the IP length and is not
# part of `payload`, so `payload` is not a suffix of `raw` here.
short = ethernet(ipv4(17, struct.pack(">HHHH", 5353, 5353, 12, 0) + b"ping"))
padded = short + b"\xee" * (60 - len(short))
write_pcap(out_dir / "padded.pcap", [(padded, len(padded))])

# Tunnels: IPv4 in IPv6 (next header 4) and IPv6 in IPv4 (protocol 41). The IP
# columns describe the outer header, not the tunneled packet, and the ports are
# not taken from the inner transport layer.
tunnels = [
    ethernet(ipv6(4, ipv4(6, tcp)), ether_type=0x86DD),
    ethernet(ipv4(41, ipv6(17, udp))),
]
write_pcap(out_dir / "tunnel.pcap", [(packet, len(packet)) for packet in tunnels])

# An untagged outer frame tunneling a VLAN-tagged tenant frame in VXLAN (UDP port
# 4789, VNI 100). The Ethernet columns and `vlan_id` describe the outer link layer,
# so `vlan_id` is `NULL` and the MAC addresses are the outer ones, even if the
# inner frame is decoded.
vxlan_header = bytes.fromhex("08000000") + struct.pack(">I", 100 << 8)
inner_frame = ethernet(ipv4(6, tcp), vlan_id=99, addresses="020000000002020000000001")
vxlan_payload = vxlan_header + inner_frame
vxlan = ethernet(ipv4(17, struct.pack(">HHHH", 54321, 4789, 8 + len(vxlan_payload), 0) + vxlan_payload))
write_pcap(out_dir / "vxlan.pcap", [(vxlan, len(vxlan))])

# Raw IP captures have no link-layer header: `LINKTYPE_RAW` (101) carries IPv4
# and IPv6 packets told apart by the version nibble, while `LINKTYPE_IPV4` (228)
# and `LINKTYPE_IPV6` (229) carry only one version each.
raw_ip = [ipv4(6, tcp), ipv6(17, udp), ipv4(1, icmp)]
write_pcap(out_dir / "raw_ip.pcap", [(packet, len(packet)) for packet in raw_ip], linktype=101)
write_pcap(out_dir / "ipv4.pcap", [(ipv4(17, udp), len(ipv4(17, udp)))], linktype=228)
write_pcap(out_dir / "ipv6.pcap", [(ipv6(6, tcp), len(ipv6(6, tcp)))], linktype=229)

# Link types dissected by `libtins` other than Ethernet. BSD loopback
# (`LINKTYPE_NULL`, 0) prefixes the packet with a 4-byte address family in the
# byte order of the capturing host, here little-endian `AF_INET` (2). Linux
# cooked capture (`LINKTYPE_LINUX_SLL`, 113) prefixes it with a 16-byte header:
# packet type (unicast to us), ARPHRD type (Ethernet), address length, the
# address padded to 8 bytes, and the EtherType (IPv4).
loopback = struct.pack("<I", 2) + ipv4(6, tcp)
write_pcap(out_dir / "null.pcap", [(loopback, len(loopback))], linktype=0)
sll = struct.pack(">HHH", 0, 1, 6) + bytes.fromhex("0011223344550000") + struct.pack(">H", 0x0800) + ipv4(17, udp)
write_pcap(out_dir / "sll.pcap", [(sll, len(sll))], linktype=113)

# The same packets cut by the snapshot length inside the IPv4 header, and an
# Ethernet frame behind a PPI header (`LINKTYPE_PPI`, 192: version, flags, header
# length and the link type of the carried frame, little-endian) in full and cut
# inside the IPv4 header. The outer headers that were captured completely are
# kept and the rest is raw, as for Ethernet frames.
write_pcap(out_dir / "truncated_null.pcap", [(loopback[:4 + 10], len(loopback))], snaplen=14, linktype=0)
write_pcap(out_dir / "truncated_sll.pcap", [(sll[:16 + 10], len(sll))], snaplen=26, linktype=113)
ppi = struct.pack("<BBHI", 0, 0, 8, 1) + packets[0]
write_pcap(out_dir / "ppi.pcap", [(ppi, len(ppi)), (ppi[:8 + 18], len(ppi))], linktype=192)

# A link type that is not dissected (`LINKTYPE_LINUX_SLL2`, 276): the packet
# still produces a row with the capture metadata and `raw`, but no layer columns.
sll2 = bytes.fromhex("0800000000000001000106") + bytes(9) + ipv4(17, udp)
write_pcap(out_dir / "sll2.pcap", [(sll2, len(sll2))], linktype=276)

# IEEE 802.3 frames: the type field is the length of the frame data (at most
# 1500) and the data starts with an LLC header. With both SAPs 0xAA and control
# 0x03, a SNAP header (OUI 000000, EtherType 0x0800) carries an IPv4 packet; the
# second frame is a plain LLC frame (SAPs 0x42, an STP BPDU) padded to the
# 60-byte minimum. The third frame is Ethernet II for comparison.
def dot3(llc_data, addresses="0180c20000000011223344aa"):
    return bytes.fromhex(addresses) + struct.pack(">H", len(llc_data)) + llc_data


snap_ip = dot3(bytes.fromhex("aaaa03000000") + struct.pack(">H", 0x0800) + ipv4(17, udp))
stp = dot3(bytes.fromhex("424203") + bytes(35))
stp_padded = stp + b"\xee" * (60 - len(stp))
dot3_frames = [snap_ip, stp_padded, ethernet(ipv4(17, udp))]
write_pcap(out_dir / "dot3.pcap", [(packet, len(packet)) for packet in dot3_frames])

# The IEEE 802.3 frame with SNAP cut by the snapshot length right after the SNAP
# header and inside the IPv4 header it carries: the LLC header with SNAP is kept,
# the rest is raw, and `eth_type` still names IP from the SNAP header.
truncated_dot3 = [(snap_ip[:14 + 8], len(snap_ip)), (snap_ip[:14 + 8 + 10], len(snap_ip))]
write_pcap(out_dir / "truncated_dot3.pcap", truncated_dot3, snaplen=32)

# Headers with padding after the end-of-options marker, which a reader must skip
# through the length field of the header (RFC 791, section 3.1, and RFC 9293):
# an IPv4 header with `IHL = 6` whose options are the end-of-options marker and
# three bytes of padding, carrying UDP; an IPv4 TCP packet whose TCP header has
# `data offset = 6` and the same options; and both at once. The payload is `ping`
# in every packet, and the address and port columns are filled.
def ipv4_with_options(protocol, options, payload):
    assert len(options) % 4 == 0
    header_length = 20 + len(options)
    return (
        bytes([0x40 | (header_length // 4), 0])
        + struct.pack(">HHH", header_length + len(payload), 0, 0x4000)
        + bytes.fromhex("40")
        + bytes([protocol])
        + bytes.fromhex("0000c0000201c0000202")
        + options
        + payload
    )


end_of_options = bytes(4)
udp_ping = struct.pack(">HHHH", 5353, 5353, 12, 0) + b"ping"
tcp_ping = struct.pack(">HHIIBBHHH", 12345, 80, 1, 0, 0x60, 0x18, 1024, 0, 0) + end_of_options + b"ping"
padded_options = [
    ethernet(ipv4_with_options(17, end_of_options, udp_ping)),
    ethernet(ipv4(6, tcp_ping)),
    ethernet(ipv4_with_options(6, end_of_options, tcp_ping)),
]

# A non-first IPv6 fragment (offset 2) of a datagram with a Destination Options
# header before UDP: the Fragment header names Destination Options (60), but that
# header is in the first fragment, and the bytes after the Fragment header are
# UDP application data that happen to look like a Destination Options header
# naming TCP. Only the type named by the Fragment header is known.
non_first_fragment = bytes([60, 0]) + struct.pack(">HI", 2 << 3, 0x12345678) + bytes.fromhex("0600000000000000") + b"data"
padded_options.append(ethernet(ipv6(44, non_first_fragment), ether_type=0x86DD))
write_pcap(out_dir / "options.pcap", [(packet, len(packet)) for packet in padded_options])

# A raw IPv4 header with options (`IHL = 6`) cut by the snapshot length inside
# the options, after both addresses: the header is not dissected, but the
# version, the TTL, the protocol and the addresses are in the captured bytes.
ip_with_options = ipv4_with_options(17, end_of_options, udp_ping)
write_pcap(out_dir / "truncated_ip_options.pcap", [(ip_with_options[:22], len(ip_with_options))], snaplen=22, linktype=101)

# First fragments (fragment offset 0, "more fragments" set): the transport
# header is in this fragment, so the ports, the TCP fields, and the part of the
# payload in this fragment are decoded. An IPv4 TCP first fragment, and an IPv6
# UDP first fragment behind a Fragment header (offset 0, M = 1). The UDP length
# names the whole datagram, which is longer than this fragment.
first_fragments = [
    ethernet(ipv4(6, tcp, flags_and_offset=0x2000)),
    ethernet(
        ipv6(44, bytes([17, 0]) + struct.pack(">HI", 1, 0x12345678) + struct.pack(">HHHH", 5353, 5353, 1008, 0) + b"first"),
        ether_type=0x86DD,
    ),
]
write_pcap(out_dir / "fragments.pcap", [(packet, len(packet)) for packet in first_fragments])
