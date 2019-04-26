#!/bin/sh

set -e
SCRIPTPATH=$(readlink -f "$0")
SCRIPTDIR=$(dirname "$SCRIPTPATH")
PYTHONPATH="$SCRIPTDIR" python3 -m github "$@"
