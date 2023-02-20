#!/bin/bash

set -e

# We check only our code, that's why we skip contrib
GIT_ROOT=$(git rev-parse --show-cdup)
GIT_ROOT=${GIT_ROOT:-.}
VERSION=$(sed -e '1 s/^v//; 1 s/-.*//p; d' "$GIT_ROOT"/utils/list-versions/version_date.tsv)

find "$GIT_ROOT/docker/keeper/" "$GIT_ROOT/docker/server/" -name 'Dockerfile.*' -print0 | \
  xargs -0 sed -i --follow-symlinks "/^ARG VERSION=/ s/^.*$/ARG VERSION=\"$VERSION\"/"
