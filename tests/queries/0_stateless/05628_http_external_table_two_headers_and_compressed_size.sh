#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_NAME="test_ext_two_headers_user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER_NAME}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER_NAME} IDENTIFIED WITH no_password SETTINGS http_max_multipart_form_data_size = 1000"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.* TO ${USER_NAME}"

# 1. A part with two MIME headers (`Content-Disposition` and `Content-Type`) and a tiny
# `http_max_multipart_form_data_size`. The second header line is request syntax, bounded by
# `http_max_request_header_size`, and must not be taken for content and rejected by the much
# smaller content limit.
$CLICKHOUSE_CLIENT -q "ALTER USER ${USER_NAME} SETTINGS http_max_multipart_form_data_size = 8"

BOUNDARY="----------boundary04a6f4b1d2c3"
printf -- '--%s\r\nContent-Disposition: form-data; name="ext"; filename="data"\r\nContent-Type: application/octet-stream\r\n\r\n1\n2\n3\n\r\n--%s--\r\n' "${BOUNDARY}" "${BOUNDARY}" | \
    ${CLICKHOUSE_CURL} -sS -H "Content-Type: multipart/form-data; boundary=${BOUNDARY}" --data-binary @- \
    "${CLICKHOUSE_URL}&user=${USER_NAME}&query=SELECT+sum(id)+FROM+ext&ext_structure=id+UInt8&ext_format=TSV"

# 2. A header-only native-compressed block (a zero checksum and an LZ4 header) that declares a tiny
# decompressed size but a 512 MiB compressed size. It must be rejected by the size check before a
# buffer for the declared compressed block is allocated, not after trying to read the missing data.
$CLICKHOUSE_CLIENT -q "ALTER USER ${USER_NAME} SETTINGS http_max_multipart_form_data_size = 1000"

printf '\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\202\000\000\000\040\001\000\000\000' | \
    ${CLICKHOUSE_CURL} -sSF 'ext=@-' "${CLICKHOUSE_URL}&user=${USER_NAME}&query=SELECT+count()+FROM+ext&ext_structure=id+UInt8&ext_format=Native&ext_decompress=1" 2>&1 | \
    grep -o 'Too large compressed size: [0-9]*, the maximum is: 1000' | head -n1

$CLICKHOUSE_CLIENT -q "DROP USER ${USER_NAME}"
