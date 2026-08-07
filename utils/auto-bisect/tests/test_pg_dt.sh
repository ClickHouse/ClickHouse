#!/bin/bash
set -e

WORK_TREE="$1"
(
  cd $WORK_TREE || exit 129

  $CH_PATH client --user admin  -q "create table test (ts DateTime64(6, 'UTC'), d Date) engine = PostgreSQL('localhost:15432', 'postgres', 'test', 'postgres', 'password');
  select ts from test where ts > '2025-01-01 00:00:00'::DateTime64" || exit 1
  # Run some test
#    ./tests/clickhouse-test --no-random-settings --no-random-merge-tree-settings 01413_rows_events.sql
  # Or run custom script
  # clickhouse client -q "drop database if exists x; create database x; create table x.ttl (d Date, a Int) engine = MergeTree order by a partition by toDayOfMonth(d); insert into x.ttl values (toDateTime('2000-10-10 00:00:00'), 1)"
)
