#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The Web UI pages embedded in the server reference third-party JavaScript and CSS on CDNs (with
# subresource integrity), so they also work when opened as local files. When served by ClickHouse,
# these links must be rewritten to the copies embedded in the binary, so no third-party code runs in
# the ClickHouse HTTP origin.

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

for page in play dashboard binary jemalloc docs merges schema processors-profile
do
    html=$(${CLICKHOUSE_CURL} -sS "${URL}/${page}")
    # The page is served ...
    echo "${page}" "$(echo "${html}" | grep -c '</html>')"
    # ... and references no CDN, and no integrity hashes are left over from the rewritten links.
    echo "${html}" | grep -n -E 'cdn\.jsdelivr\.net|unpkg\.com|cdnjs\.cloudflare\.com|integrity=' ||:
done

# The rewritten links point to the embedded copies, relative to the page (so that they also work behind a
# reverse proxy that exposes the server under a path prefix).
${CLICKHOUSE_CURL} -sS "${URL}/binary" | grep -oE '"\./js/leaflet\.(js|css)"'
${CLICKHOUSE_CURL} -sS "${URL}/jemalloc" | grep -oE '"\./js/uplot(\.min\.css|\.js)"'
${CLICKHOUSE_CURL} -sS "${URL}/dashboard" | grep -oE '"\./js/(uplot|lz-string)\.js"'

# The embedded copies are served.
${CLICKHOUSE_CURL} -sS "${URL}/js/leaflet.js" | grep -oF 'Leaflet 1.9.4' | head -n1
${CLICKHOUSE_CURL} -sS "${URL}/js/leaflet.css" | grep -oF '.leaflet-container' | head -n1
${CLICKHOUSE_CURL} -sS "${URL}/js/uplot.min.css" | grep -oF '.uplot' | head -n1
