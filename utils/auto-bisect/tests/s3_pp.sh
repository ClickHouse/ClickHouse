#!/bin/bash
set -e
#./bisect.sh --test tests/test_rmv_definer.sh --good af9a4c6a --bad b7c20bd6  --path /home/nik/work/clickhouse-private1/ --env nothing

(
  cd $GIT_WORK_TREE/tests/integration || exit 1
  echo "$PWD"

  export CLICKHOUSE_TESTS_SERVER_BIN_PATH=$CH_PATH
  # ODBC bridge isn't used
  export CLICKHOUSE_TESTS_ODBC_BRIDGE_BIN_PATH=$CH_PATH
  export CLICKHOUSE_TESTS_LIBRARY_BRIDGE_BIN_PATH=$CH_PATH

  # If you want to reproduce it, copy the test directory and do not track it with git, otherwise you will encounter git conflicts

  if ./runner test_storage_s3_pp/test_sts_smoke.py 2>&1 | tee /dev/tty | grep -q "Distributed task iterator is not initialized"; then
    echo "Exiting with 1."
    exit 1
  else
    echo "Exiting with 0."
    exit 0
  fi

)