#!/bin/bash
# do not install clickhouse, used for client bisecting on same CH version
set -e

if [ -z "$1" ]; then
  echo "Usage: $0 <WORK_TREE>"
  exit 1
fi

WORK_TREE="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check if the script is located in a directory named 'env'
if [ "$(basename "$SCRIPT_DIR")" = "env" ]; then
  SCRIPT_DIR="$(dirname "$SCRIPT_DIR")"
fi

CH_PATH=${CH_PATH:=$(command -v clickhouse || true)}

if [ -z "$CH_PATH" ] || [ ! -s "$CH_PATH" ]; then
  echo "Can't find clickhouse binary at '$CH_PATH'"
  exit 1
fi

rm -rf $SCRIPT_DIR/data/ch
