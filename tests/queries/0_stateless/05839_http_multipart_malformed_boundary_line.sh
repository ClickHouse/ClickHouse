#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A line of a multipart/form-data message that starts with the boundary is a delimiter line only
# if nothing but the "--" of the close delimiter and linear whitespace (RFC 2046 transport padding)
# follows the boundary. A line that starts with the boundary and continues with anything else is
# malformed input and is rejected: taking it for a delimiter would silently drop the rest of the
# line and truncate the part. All the lines here are far below the size limits.

URL="${CLICKHOUSE_URL}&query=SELECT+count(),+sum(length(s))+FROM+ext&ext_structure=s+String&ext_format=TSV"

BOUNDARY="bnd"

# $1 - the bytes that follow the first boundary on its line.
# $2 - the content of the part.
# $3 - the bytes that follow the closing boundary on its line.
send_multipart()
{
    {
        printf -- '--%s%b\r\n' "${BOUNDARY}" "$1"
        printf 'Content-Disposition: form-data; name="ext"; filename="data"\r\n\r\n'
        printf '%b' "$2"
        printf -- '\r\n--%s%b\r\n' "${BOUNDARY}" "$3"
    } | ${CLICKHOUSE_CURL} -sS -X POST -H "Content-Type: multipart/form-data; boundary=${BOUNDARY}" --data-binary @- "${URL}"
}

echo 'Valid delimiter lines'
send_multipart '' 'abc\ndef' '--'
send_multipart ' \t ' 'abc\ndef' '-- \t '
# The boundary inside a line, rather than at its beginning, is content.
send_multipart '' "abc\nx--${BOUNDARY}garbage\ndef" '--'
# So is the boundary after a bare LF: a delimiter line starts after CRLF.
send_multipart '' "abc\n--${BOUNDARY}garbage\ndef" '--'

echo 'Malformed delimiter lines'
# A line of the content that starts with the boundary: the data after it must not be dropped.
send_multipart '' "abc\r\n--${BOUNDARY}garbage\r\ndef" '--' 2>&1 | grep -o 'Malformed boundary line' | head -n1
send_multipart '' "abc\r\n--${BOUNDARY}--garbage\r\ndef" '--' 2>&1 | grep -o 'Malformed boundary line' | head -n1
# The first boundary line.
send_multipart 'garbage' 'abc\ndef' '--' 2>&1 | grep -o 'Malformed boundary line' | head -n1
# The closing boundary line.
send_multipart '' 'abc\ndef' 'garbage' 2>&1 | grep -o 'Malformed boundary line' | head -n1
send_multipart '' 'abc\ndef' '-' 2>&1 | grep -o 'Malformed boundary line' | head -n1
send_multipart '' 'abc\ndef' '---' 2>&1 | grep -o 'Malformed boundary line' | head -n1
send_multipart '' 'abc\ndef' ' --' 2>&1 | grep -o 'Malformed boundary line' | head -n1
