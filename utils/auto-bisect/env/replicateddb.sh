#!/bin/bash
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

source ${SCRIPT_DIR}/helpers/lib.sh
start_minio

ETC_DIR="$SCRIPT_DIR/data/etc"
CH_CONFIG_DIR="$ETC_DIR/clickhouse-server"
CH_CONFIG_DIR1="$ETC_DIR/clickhouse-server1"
CH_CLIENT_DIR="$ETC_DIR/clickhouse-client"

(
  cd $WORK_TREE || exit 1
  rm -rf "$CH_CONFIG_DIR" "$CH_CONFIG_DIR1"
  mkdir -p "$CH_CONFIG_DIR/config.d" "$CH_CONFIG_DIR/users.d" \
            "$CH_CONFIG_DIR1/config.d" "$CH_CONFIG_DIR1/users.d" \
            "$CH_CLIENT_DIR" \
            /var/lib/clickhouse/ /var/lib/clickhouse1/

  # Copy base server configs, dereferencing symlinks so absolute paths are not needed
  cp -rL programs/server/. "$CH_CONFIG_DIR/"

  # Remove configs that conflict with or are superseded by our replicated setup
  rm -f \
    "$CH_CONFIG_DIR/config.d/keeper_port.xml" \
    "$CH_CONFIG_DIR/config.d/azure_storage_conf.xml" \
    "$CH_CONFIG_DIR/config.d/azure_storage_policy_by_default.xml" \
    "$CH_CONFIG_DIR/config.d/zookeeper.xml" \
    "$CH_CONFIG_DIR/config.d/keeper.xml" \
    "$CH_CONFIG_DIR/config.d/keeper_port_fault_injection.xml" \
    "$CH_CONFIG_DIR/config.d/distributed_cache_server.xml" \
    "$CH_CONFIG_DIR/config.d/distributed_cache_client.xml" \
    "$CH_CONFIG_DIR/config.d/reduce_blocking_parts.xml" \
    "$CH_CONFIG_DIR/config.d/database_replicated.xml" \
    "$CH_CONFIG_DIR/config.d/clusters.xml"

  # Add our replicated setup configs
  cp $SCRIPT_DIR/env/config/database_replicated.xml "$CH_CONFIG_DIR/config.d/"
  cp $SCRIPT_DIR/env/config/clusters.xml "$CH_CONFIG_DIR/config.d/"
  cp $SCRIPT_DIR/env/config/users_cloud.xml "$CH_CONFIG_DIR/users.d/"
  cp $WORK_TREE/tests/config/users.d/database_replicated.xml "$CH_CONFIG_DIR/users.d/"

  # Override filesystem caches path to use /var/lib/clickhouse
  cp $SCRIPT_DIR/env/config/filesystem_caches_path.xml "$CH_CONFIG_DIR/config.d/"

  # Copy server1 config to server2
  cp -r "$CH_CONFIG_DIR/." "$CH_CONFIG_DIR1/"

  # Patch server2: replica r1 -> r2
  sed "s|<replica>r1</replica>|<replica>r2</replica>|" \
    "$CH_CONFIG_DIR/config.d/macros.xml" \
    > "$CH_CONFIG_DIR1/config.d/macros.xml"

  # Patch server2: separate transaction log path
  sed "s|/test/clickhouse/txn|/test/clickhouse/txn1|" \
    "$CH_CONFIG_DIR/config.d/transactions.xml" \
    > "$CH_CONFIG_DIR1/config.d/transactions.xml"

  # Patch server2: separate filesystem cache path
  sed \
    -e "s|<filesystem_caches_path>/var/lib/clickhouse/filesystem_caches/</filesystem_caches_path>|<filesystem_caches_path>/var/lib/clickhouse/filesystem_caches_1/</filesystem_caches_path>|" \
    -e "s|<custom_cached_disks_base_directory replace=\"replace\">/var/lib/clickhouse/filesystem_caches/</custom_cached_disks_base_directory>|<custom_cached_disks_base_directory replace=\"replace\">/var/lib/clickhouse/filesystem_caches_1/</custom_cached_disks_base_directory>|" \
    "$CH_CONFIG_DIR/config.d/filesystem_caches_path.xml" \
    > "$CH_CONFIG_DIR1/config.d/filesystem_caches_path.xml"
)


PID_FILE="$SCRIPT_DIR/data/ch.pid"
PID_FILE1="$SCRIPT_DIR/data/ch1.pid"
LOG_FILE="$SCRIPT_DIR/data/clickhouse.log"
LOG_FILE1="$SCRIPT_DIR/data/clickhouse1.log"
CH_DATA="$SCRIPT_DIR/data/ch"
CH_DATA1="$SCRIPT_DIR/data/ch1"

# Kill all local ClickHouse servers
(ps aux | grep -E 'clickhouse[- ]server' | awk '{print $2}' | xargs kill -9) 2>/dev/null || true
sleep 1

if [[ "${CLEAN_CH_DATA:-1}" -ne 0 ]]; then
  rm -rf $CH_DATA $CH_DATA1 $LOG_FILE $LOG_FILE1
fi
mkdir -p $CH_DATA $CH_DATA1

function start_ch()
{
  (
    cd $CH_DATA || exit 1
    kill -9 "$(cat $PID_FILE 2>/dev/null)" 2>/dev/null || true
    rm -f $PID_FILE
    $CH_PATH server --config "$CH_CONFIG_DIR/config.xml" --pid-file=$PID_FILE \
      -- --path="$CH_DATA" > "$LOG_FILE" 2>&1 &
  )
}

function start_ch2()
{
  (
    cd $CH_DATA1 || exit 1
    kill -9 "$(cat $PID_FILE1 2>/dev/null)" 2>/dev/null || true
    rm -f $PID_FILE1
    $CH_PATH server --config "$CH_CONFIG_DIR1/config.xml" \
      --pid-file $PID_FILE1 \
      -- --path="$CH_DATA1" \
         --logger.errorlog "$LOG_FILE1" \
         --tcp_port 19000 --tcp_port_secure 19440 --http_port 18123 --https_port 18443 \
         --interserver_http_port 19009 --tcp_with_proxy_port 19010 \
         --mysql_port 19004 --postgresql_port 19005 \
         --keeper_server.tcp_port 19181 --keeper_server.server_id 2 \
         --prometheus.port 19988 --macros.replica r2 > "$LOG_FILE1" 2>&1 &
  )
}

start_ch
start_ch2

wait_ch_start $CH_PATH 9000
wait_ch_start $CH_PATH 19000
