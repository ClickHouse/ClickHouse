#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the PCAP format requires libtins/libpcap, which are not built in fasttest (ENABLE_LIBRARIES=0).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The contents of a packet choose how many layers it has, and a header takes only a few bytes,
# so a single packet can name hundreds of thousands of layers nested one inside another.
# Such a packet is dissected up to a fixed number of layers, and the rest of it is kept raw.

DATA_DIR="$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}_pcap"
trap 'rm -rf "$DATA_DIR"' EXIT
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"
python3 "$CUR_DIR/data_pcap/generate.py" "$DATA_DIR"

echo "--- PPI headers that name PPI as the link type of the frame they carry ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, capture_length, link_type, length(protocols), arrayMap(x -> (x, countEqual(protocols, x)), arrayDistinct(protocols)), length(raw)
FROM file('$DATA_DIR/nested_ppi.pcapng', PCAP)
ORDER BY number FORMAT TSV"

echo "--- 802.1Q tags, MPLS labels, 802.1Q tags of an IEEE 802.3 frame, IPv4 in IPv4 ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, capture_length, link_type, length(protocols), arrayMap(x -> (x, countEqual(protocols, x)), arrayDistinct(protocols)), eth_type, vlan_id, ip_version, ip_protocol, src_port, payload_length, length(raw)
FROM file('$DATA_DIR/nested.pcapng', PCAP)
ORDER BY number FORMAT TSV"
