# PCAP test captures

`generate.py <output directory>` creates the captures used by
`04498_pcap_input_format.sh` from self-authored Ethernet, IPv4, TCP, UDP, and
ICMP packets. It uses only the Python standard library. The test generates them
into its own temporary directory, so that concurrent runs of the test do not
rewrite each other's files.

- `packets.pcap` — classic `pcap`, full packets: TCP, UDP, ICMP, a VLAN-tagged
  TCP packet, and a non-first IPv4 fragment.
- `packets.pcapng` — the same packets in a `pcapng` container.
- `truncated.pcap` — the first three packets captured with a snapshot length of
  34 bytes, so `original_length > capture_length`.
