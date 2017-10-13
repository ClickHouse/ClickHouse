#!/bin/sh -x
# Usages:
# ./clickhouse_report.sh > ch.`hostname`.`date '+%Y%M%''d%H%M%''S'`.dmp 2>&1
# curl https://raw.githubusercontent.com/yandex/ClickHouse/master/utils/report/clickhouse_report.sh | sh > ch.`hostname`.`date '+%Y%M%''d%H%M%''S'`.dmp 2>&1

clickhouse --client -q 'SELECT * FROM system.events FORMAT Pretty'
clickhouse --client -q 'SELECT * FROM system.metrics FORMAT Pretty'
clickhouse --client -q 'SELECT * FROM system.asynchronous_metrics FORMAT Pretty'
clickhouse --client -q 'SELECT * FROM system.build_options FORMAT Pretty'
clickhouse --client -q 'SELECT * FROM system.processes FORMAT Pretty'
clickhouse --client -q 'SELECT * FROM system.merges FORMAT Pretty'
clickhouse --client -q 'SELECT * FROM system.parts FORMAT Pretty'
clickhouse --client -q 'SELECT * FROM system.replication_queue FORMAT Pretty'
clickhouse --client -q 'SELECT * FROM system.dictionaries FORMAT Pretty'
ps auxw
df -h
top -bn1
tail -n200 /var/log/clickhouse-server/clickhouse-server.err.log
tail -n200 /var/log/clickhouse-server/clickhouse-server.log
tail -n100 /var/log/clickhouse-server/stderr
cat /etc/lsb-release
uname -a
