# PCAP test captures

`generate.py <output directory>` creates the captures used by
`04498_pcap_input_format.sh` from self-authored Ethernet, IPv4, IPv6, TCP, UDP,
and ICMP packets. It uses only the Python standard library. The test generates them
into its own temporary directory, so that concurrent runs of the test do not
rewrite each other's files.

- `packets.pcap` — classic `pcap`, full packets: TCP, UDP, ICMP, a VLAN-tagged
  TCP packet, a non-first IPv4 fragment, native IPv6 TCP and UDP, and IPv6
  packets behind extension headers (Hop-by-Hop and Routing, Fragment,
  Authentication Header, Shim6).
- `packets.pcapng` — the same packets in a `pcapng` container.
- `truncated.pcap` — the first three packets captured with a snapshot length of
  34 bytes, so `original_length > capture_length`.
- `truncated_headers.pcap` — the IPv6 packet with the Hop-by-Hop and Routing
  chain cut inside the Hop-by-Hop header, inside the Routing header, and inside
  the TCP header, and an IPv4 TCP packet cut inside the TCP header, so the
  packets are dissected up to the last header captured completely.
- `truncated_ip.pcap`, `truncated_raw_ip.pcap` — Ethernet frames cut inside the
  IPv4 header, the IPv6 header, and a VLAN tag, and raw IP packets cut inside the
  IPv4 and the IPv6 header, so the packets are dissected up to the Ethernet
  header, if there is one, and the rest is raw.
- `padded.pcap` — a UDP packet with a 4-byte payload in an Ethernet frame padded
  to the 60-byte minimum, so `raw` ends with padding bytes that are not part of
  `payload`.
- `tunnel.pcap` — IPv4 TCP tunneled in IPv6 and IPv6 UDP tunneled in IPv4, so the
  IP columns must describe the outer header and not the tunneled packet.
- `vxlan.pcap` — an untagged Ethernet frame tunneling a VLAN-tagged Ethernet
  frame in VXLAN, so the Ethernet columns and `vlan_id` must describe the outer
  link layer and not the tunneled frame.
- `null.pcap`, `sll.pcap` — an IPv4 TCP packet behind a BSD loopback header
  (`LINKTYPE_NULL`) and an IPv4 UDP packet behind a Linux cooked capture header
  (`LINKTYPE_LINUX_SLL`), link types that are dissected by `libtins`.
- `truncated_null.pcap`, `truncated_sll.pcap`, `ppi.pcap` — the `null.pcap` and
  `sll.pcap` packets cut inside the IPv4 header, and an Ethernet frame behind a
  PPI header (`LINKTYPE_PPI`) in full and cut inside the IPv4 header, so the
  complete outer headers are kept and the rest is raw.
- `truncated_dot3.pcap` — the IEEE 802.3 frame with `SNAP` from `dot3.pcap` cut
  right after the `SNAP` header and inside the IPv4 header it carries, so the
  `SNAP` header is kept, the rest is raw, and `eth_type` is still `IP`.
- `options.pcap` — IPv4 and TCP headers whose options are the end-of-options
  marker followed by padding, so the payload starts at the captured header
  length, and a non-first IPv6 fragment whose data looks like an extension
  header, so the extension-header chain stops at the Fragment header.
- `fragments.pcap` — a first IPv4 fragment of a TCP segment and a first IPv6
  fragment of a UDP datagram (fragment offset 0, more fragments), so the
  transport header in them is decoded.
