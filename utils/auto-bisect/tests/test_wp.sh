#!/bin/bash
# not working!!!

export CH_PATH=$(realpath ../../clickhouse2410)
export DATA_DIR="/home/nik/chdata"
set -e

#CH_PATH=../../clickhouse2412
./env/replicated.sh /home/nik/work/clickhouse-private1/

$CH_PATH client --query "
  SELECT version();
  CREATE DATABASE x;
  CREATE TABLE x.src (x UInt64) ENGINE = SharedReplacingMergeTree ORDER BY x;
  --SETTINGS shared_merge_tree_disable_merges_and_mutations_assignment=1;
  --SYSTEM STOP MERGES ON CLUSTER default x.src;
"

head -c 1000 /dev/zero | $CH_PATH client --insert_deduplicate=0 --max_insert_block_size=1 --min_insert_block_size_rows=1 --min_insert_block_size_bytes=1 -q "INSERT INTO x.src FORMAT RowBinary"
echo 'insert done'
#$CH_PATH client --query "
#  ALTER TABLE x.src MODIFY SETTING shared_merge_tree_disable_merges_and_mutations_assignment=0;
#  SYSTEM START MERGES ON CLUSTER default x.src;
#"
echo 'alter done'
#
#for ((i = 1; i <= 1000; i++)); do
#  head -c 1000 /dev/zero | $CH_PATH client --insert_deduplicate=0 --max_insert_block_size=1 --min_insert_block_size_rows=1 --min_insert_block_size_bytes=1 -q "INSERT INTO x.src FORMAT RowBinary"
#done
#echo 'insert done'

wait
exit 1

$CH_PATH client --query "SELECT count() FROM x.src;
  SELECT count() FROM system.parts WHERE table='src';
  SELECT * FROM system.virtual_parts WHERE table='src';
"

# do not remove clickhouse data
export CLEAN_CH_DATA=0
CH_PATH=$(realpath ../../clickhouse2412)
./env/replicated.sh /home/nik/work/clickhouse-private1/

head -c 1000 /dev/zero | $CH_PATH client --insert_deduplicate=0 --max_insert_block_size=1 --min_insert_block_size_rows=1 --min_insert_block_size_bytes=1 -q "INSERT INTO x.src FORMAT RowBinary"

$CH_PATH client --query "SELECT count() FROM x.src;
  SELECT count() FROM system.parts WHERE table='src';
  SELECT * FROM system.virtual_parts WHERE table='src';
"
