# PCAP test captures

These capture files are used by `04498_pcap_input_format.sh` to test the
`PCAP` input format.

They are sample captures taken from the Wireshark test suite:
https://github.com/wireshark/wireshark/tree/master/test/captures

| File | Contents |
|------|----------|
| `http.pcap` | A single Ethernet/IPv4/TCP packet carrying an HTTP request |
| `dhcp.pcapng` | DHCP exchange over Ethernet/IPv4/UDP (ports 67/68) |
| `icmp_ascii.pcapng` | ICMP echo request/reply packets |
| `tls13-20-chacha20poly1305.pcap` | A TLS 1.3 session over Ethernet/IPv4/TCP |

Wireshark is licensed under GPL-2.0. These files are captured packet data
rather than program source, and are included here only as read-only test
fixtures.
