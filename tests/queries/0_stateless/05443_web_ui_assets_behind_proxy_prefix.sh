#!/usr/bin/env bash

# The built-in web UI pages load the JavaScript and CSS files embedded into the server from `js/`.
# A reverse proxy can expose the server under a path prefix, as `https://proxy.example/clickhouse/`,
# where `https://proxy.example/` is an unrelated site, so a root-absolute `/js/...` link would load
# nothing - and a page whose scripts fail to load does not run at all. The links have to be relative
# to the page, for every route suffix the page is served under (the routes are non-strict paths).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SERVER="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
PROXY="https://proxy.example/clickhouse"

for route in 'docs' 'docs/' 'docs/a/b?x=/y' 'dashboard' 'dashboard/' 'webterminal' 'webterminal/'
do
    # The links to the embedded files, as the browser sees them in the served page.
    links=$(${CLICKHOUSE_CURL} -sS "${SERVER}/${route}" | grep -oE '(src|href)="[^"]*js/[^"]*"' | grep -vF '"http' | sed -E 's/^(src|href)="(.*)"$/\2/')

    for link in ${links}
    do
        # Where the link leads behind the proxy, and whether the server serves it when there is no proxy.
        behind_proxy=$(python3 -c 'import sys, urllib.parse; print(urllib.parse.urljoin(sys.argv[1], sys.argv[2]))' "${PROXY}/${route}" "${link}")
        direct=$(python3 -c 'import sys, urllib.parse; print(urllib.parse.urljoin(sys.argv[1], sys.argv[2]))' "${SERVER}/${route}" "${link}")
        status=$(${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}' "${direct}")
        echo "${route} ${behind_proxy#https://proxy.example} ${status}"
    done
done

# `/play` and `/processors-profile` load their embedded scripts from JavaScript, relative to the
# address of the page without the route name, rather than from the site root.
for route in play processors-profile
do
    content=$(${CLICKHOUSE_CURL} -sS "${SERVER}/${route}")
    echo "${content}" | grep -qF "'/js/" && echo "${route} root-absolute" || echo "${route} relative"
done
