#!/usr/bin/env python3
import sys
from testflows.core import *

append_path(sys.path, "..") 

from helpers.cluster import Cluster
from helpers.argparser import argparser
from rbac.requirements import *

issue_12507 = "https://github.com/ClickHouse/ClickHouse/issues/12507"
issue_12510 = "https://github.com/ClickHouse/ClickHouse/issues/12510"
issue_12600 = "https://github.com/ClickHouse/ClickHouse/issues/12600"

xfails = {
    "syntax/show create quota/I show create quota current":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/12495")],
    "syntax/create role/I create role that already exists, throws exception":
        [(Fail, issue_12510)],
    "syntax/create user/I create user with if not exists, user does exist":
        [(Fail, issue_12507)],
    "syntax/create row policy/I create row policy if not exists, policy does exist":
        [(Fail, issue_12507)],
    "syntax/create quota/I create quota if not exists, quota does exist":
        [(Fail, issue_12507)],
    "syntax/create role/I create role if not exists, role does exist":
        [(Fail, issue_12507)],
    "syntax/create settings profile/I create settings profile if not exists, profile does exist":
        [(Fail, issue_12507)],
    "syntax/grant privilege/grant privileges/privilege='dictGet', on=('db0.table0', 'db0.*', '*.*', 'tb0', '*'), allow_introspection=False":
        [(Fail, issue_12600)],
    "syntax/grant privilege/grant privileges/privilege='CREATE', on=('db0.table0', 'db0.*', '*.*', 'tb0', '*'), allow_introspection=False":
        [(Fail, issue_12600)],
    "syntax/grant privilege/grant privileges/privilege='DROP', on=('db0.table0', 'db0.*', '*.*', 'tb0', '*'), allow_introspection=False":
        [(Fail, issue_12600)],
    "syntax/grant privilege/grant privileges/privilege='TRUNCATE', on=('db0.table0', 'db0.*', '*.*', 'tb0', '*'), allow_introspection=False":
        [(Fail, issue_12600)],
    "syntax/grant privilege/grant privileges/privilege='OPTIMIZE', on=('db0.table0', 'db0.*', '*.*', 'tb0', '*'), allow_introspection=False":
        [(Fail, issue_12600)],
    "syntax/grant privilege/grant privileges/privilege='SYSTEM', on=('db0.table0', 'db0.*', '*.*', 'tb0', '*'), allow_introspection=False":
        [(Fail, issue_12600)],
}

@TestModule
@ArgumentParser(argparser)
@XFails(xfails)
@Name("rbac")
def regression(self, local, clickhouse_binary_path):
    """RBAC regression.
    """
    nodes = {
        "clickhouse":
            ("clickhouse1", "clickhouse2", "clickhouse3")
    }
    with Cluster(local, clickhouse_binary_path, nodes=nodes) as cluster:
        self.context.cluster = cluster

        Feature(run=load("rbac.tests.syntax.feature", "feature"), flags=TE)

if main():
    regression()
