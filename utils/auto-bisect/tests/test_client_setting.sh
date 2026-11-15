#!/bin/bash
set -e

WORK_TREE="$1"
(
  cd $WORK_TREE/tests || exit 1

  echo "$PWD"

  $CH_PATH client --host={HOST} --port=9440  -q "select 1"

)
