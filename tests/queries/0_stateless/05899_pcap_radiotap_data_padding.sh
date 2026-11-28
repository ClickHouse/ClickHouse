#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the PCAP format requires libtins/libpcap, which are not built in fasttest (ENABLE_LIBRARIES=0).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The radiotap flag 0x20 says that the IEEE 802.11 header is padded to a multiple of four bytes.
# The padding is not a part of the carried packet: the padded frames are decoded like the
# QoS data frame without the flag (the first row), and `payload` is taken after the padding.

DATA_DIR="$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}_pcap"
trap 'rm -rf "$DATA_DIR"' EXIT
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"
python3 "$CUR_DIR/data_pcap/generate.py" "$DATA_DIR"

echo "--- QoS data without padding, QoS data with padding, four addresses with padding, QoS data with four addresses, cut inside the padding ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, capture_length, link_type, protocols, eth_type, ip_version, src_addr, dst_addr, ip_protocol, src_port, dst_port, payload_length, payload
FROM file('$DATA_DIR/radiotap_padding.pcap', PCAP)
ORDER BY number FORMAT TSV"

echo "--- the decoded columns of the padded frames are the same as of the frame without padding ---"
$CLICKHOUSE_LOCAL -q "
SELECT count(), uniqExact(tuple(eth_type, ip_version, src_addr, dst_addr, ip_protocol, ip_ttl, src_port, dst_port, payload_length, payload))
FROM file('$DATA_DIR/radiotap_padding.pcap', PCAP)
WHERE capture_length = original_length FORMAT TSV"

echo "--- the payload is a suffix of the frame ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, position(raw, payload) - 1 AS payload_offset, substring(raw, (capture_length - payload_length) + 1) = payload
FROM file('$DATA_DIR/radiotap_padding.pcap', PCAP)
ORDER BY number FORMAT TSV"
