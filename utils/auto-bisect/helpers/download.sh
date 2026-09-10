#!/bin/bash

set -u
export SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export PARENT_DIR="$(dirname "$SCRIPT_DIR")"

if [ -z "$1" ]; then
  echo "Usage: $0 <COMMIT_SHA>"
  exit 1
fi

COMMIT_SHA="$1"
CH_PATH="$PARENT_DIR/data/clickhouse"
mkdir -p "$PARENT_DIR/data"

# Build types to search for
BUILD_TYPES=("build_amd_release")
VERSIONS=("25.8" "25.9" "25.10" "25.11" "25.12" "26.1" "26.2" "26.3" "26.4" "26.5" "26.6" "26.7" "26.8" "26.9" "26.10" "26.11" "26.12")

# Public builds (default): https://clickhouse-builds.s3.amazonaws.com
# Private builds (--private): https://d1k2gkhrlfqv31.cloudfront.net/clickhouse-builds-private
#
# Private build credentials must be set via CH_CI_USER + CH_CI_PASSWORD env vars.

if [[ "${PRIVATE:-false}" == "true" ]]; then
  BASE_URL="https://d1k2gkhrlfqv31.cloudfront.net/clickhouse-builds-private"
else
  BASE_URL="https://clickhouse-builds.s3.amazonaws.com"
fi

# Build Basic Auth header from CH_CI_USER + CH_CI_PASSWORD (validated in bisect.sh)
BASIC_AUTH_HEADER=""
if [[ "${PRIVATE:-false}" == "true" ]]; then
  BASIC_AUTH_HEADER="Authorization: Basic $(printf '%s' "${CH_CI_USER}:${CH_CI_PASSWORD}" | base64 | tr -d '\n')"
fi

function try_download() {
    local URL="$1"

    # Check if URL is empty
    [[ -z "$URL" ]] && return 1

    # 1. Quick HEAD check to see if file exists (saves bandwidth/time)
    if [[ -n "$BASIC_AUTH_HEADER" ]]; then
        if ! curl -I -s -f -H "$BASIC_AUTH_HEADER" "$URL" > /dev/null; then return 1; fi
    else
        if ! curl -I -s -f "$URL" > /dev/null; then return 1; fi
    fi

    echo "Found candidate: $URL"
    rm -f "$CH_PATH"

    # 2. Download
    if [[ -n "$BASIC_AUTH_HEADER" ]]; then
        wget -q --header="$BASIC_AUTH_HEADER" -O "$CH_PATH" "$URL"
    else
        wget -q -O "$CH_PATH" "$URL"
    fi

    if [ $? -eq 0 ]; then
        chmod +x "$CH_PATH"
        # 3. Validation: binary may be self-decompressing, so allow output through
        if "$CH_PATH" --version 2>&1 | grep -q "ClickHouse"; then
            echo "Successfully verified binary from $URL"
            exit 0
        else
            echo "Warning: Downloaded file from $URL is not a valid ClickHouse binary."
            rm -f "$CH_PATH"
        fi
    fi
    return 1
}

# --- SEARCH STRATEGY ---

echo "Starting deep search for $COMMIT_SHA..."

# 1. Try Pull Request paths (High probability for recent PRs)
for BT in "${BUILD_TYPES[@]}"; do
    # Try REFs/master
    try_download "${BASE_URL}/REFs/master/${COMMIT_SHA}/${BT}/clickhouse"
    # Try direct PR paths
    try_download "${BASE_URL}/PRs/0/${COMMIT_SHA}/${BT}/clickhouse"
done

# 2. Try Versioned Release Paths
for VER in "${VERSIONS[@]}"; do
    for BT in "${BUILD_TYPES[@]}"; do
        try_download "${BASE_URL}/release/${VER}/${COMMIT_SHA}/${BT}/clickhouse"
        try_download "${BASE_URL}/${VER}/${COMMIT_SHA}/${BT}/clickhouse"
    done
done

# 3. Try "Latest" and Tag-based structures
try_download "${BASE_URL}/REFs/heads/master/${COMMIT_SHA}/build_amd_release/clickhouse"

echo "Can't find pre-built binary for commit ${COMMIT_SHA} in CI after exhaustive search."

if [ -n "${COMPILE:-}" ]; then
  echo "Falling back to local compilation..."
  "${SCRIPT_DIR}/compile.sh" "$COMMIT_SHA"
else
  exit 1
fi
