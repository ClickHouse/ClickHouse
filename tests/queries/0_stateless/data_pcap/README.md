# PCAP test captures

`generate.py` creates the captures used by `04498_pcap_input_format.sh` from
self-authored Ethernet, IPv4, TCP, UDP, and ICMP packets. It uses only the
Python standard library.

- `packets.pcap` — classic `pcap`, full packets.
- `packets.pcapng` — the same packets in a `pcapng` container.
- `truncated.pcap` — the same packets captured with a snapshot length of 34
  bytes, so `original_length > capture_length`.
