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

  # If you want to reproduce it, copy the test directory and do not track it with git otherwise, you'll encounter git conflicts
  pytest test_replicated_db_backup_restore_/test.py::test_rmv_definer
)