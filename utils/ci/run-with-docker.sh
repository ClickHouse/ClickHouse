#!/usr/bin/env bash
set -e -x

mkdir -p /var/cache/ccache
DOCKER_ENV+=" --mount=type=bind,source=/var/cache/ccache,destination=/ccache -e CCACHE_DIR=/ccache "

PROJECT_ROOT="$(cd "$(dirname "$0")/.."; pwd -P)"
[[ -n "$CONFIG" ]] && DOCKER_ENV="--env=CONFIG"
docker run -t --network=host --mount=type=bind,source=${PROJECT_ROOT},destination=/ClickHouse --workdir=/ClickHouse/ci $DOCKER_ENV "$1" "$2"
