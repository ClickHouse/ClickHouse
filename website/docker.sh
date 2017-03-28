#!/usr/bin/env bash
set -ex
BASE_DIR=$(dirname $(readlink -f $0))
IMAGE="clickhouse/website"
TAG="0.2"
FULL_NAME="${IMAGE}:${TAG}"
REMOTE_NAME="registry.yandex.net/${FULL_NAME}"
docker build -t "${FULL_NAME}" "${BASE_DIR}"
docker tag "${FULL_NAME}" "${REMOTE_NAME}"
docker push "${REMOTE_NAME}"
