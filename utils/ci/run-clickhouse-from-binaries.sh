#!/usr/bin/env bash
set -e -x

# Usage example:
# ./run-with-docker.sh centos:centos6 ./run-clickhouse-from-binaries.sh

source default-config

SERVER_BIN="${WORKSPACE}/build/src/Server/clickhouse"
SERVER_CONF="${WORKSPACE}/sources/src/Server/config.xml"
SERVER_DATADIR="${WORKSPACE}/clickhouse"

[[ -x "$SERVER_BIN" ]] || die "Run build-normal.sh first"
[[ -r "$SERVER_CONF" ]] || die "Run get-sources.sh first"

mkdir -p "${SERVER_DATADIR}"

$SERVER_BIN server --config-file "$SERVER_CONF" --pid-file="${WORKSPACE}/clickhouse.pid" -- --path "$SERVER_DATADIR"
