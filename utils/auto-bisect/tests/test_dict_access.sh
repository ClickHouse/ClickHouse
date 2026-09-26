#!/bin/bash
set -e

echo "$PWD"
CH_PATH=${CH_PATH:=clickhouse}

$CH_PATH client -q "SELECT version()";

$CH_PATH client --user admin -mn -q "
drop user if exists user;
CREATE USER user identified by '123';
drop user if exists user2;
CREATE USER user2 identified by '123';

GRANT ALL ON *.* TO user;
GRANT ALL ON *.* TO user2;
"

$CH_PATH client --user user --password 123 -mn -q "
drop database if exists x;
create database x;
drop dictionary if exists d;
drop table if exists x.tab;
create table x.tab (a UInt64, b UInt64) engine=MergeTree order by tuple() ;
insert into x.tab VALUES (1,1);

create dictionary x.d (a UInt64, b UInt64) primary key a SOURCE(CLICKHOUSE(USER 'user' PASSWORD '123' TABLE 'tab'))
LIFETIME(MIN 0 MAX 3600)
LAYOUT(FLAT())
;

-- OK
select * from x.d;
"

$CH_PATH client --user admin -mn -q "
select * from d;
"

$CH_PATH client --user user2 --password 123 -mn -q "
select * from d;
"


#result=$(echo "$result" | xargs)
## Check if the 'result' variable is not numerically equal to 10.
## We use -ne for a numerical "not equal" comparison.
#if [[ "$result" -ne 10 ]]; then
#  # If the condition is true, print an error message to standard error (>&2).
#  echo "Error: Expected result 10, but got '$result'." >&2
#
#  # Exit the script with a status code of 1 to indicate failure.
#  exit 1
#fi
