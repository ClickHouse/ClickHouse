#!/usr/bin/env python3
import sys
from testflows.core import *

append_path(sys.path, "..")

from helpers.cluster import Cluster
from helpers.argparser import argparser
from rbac.requirements import *

xfails = {
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
        Feature(run=load("rbac.tests.privileges.feature", "feature"), flags=TE)

if main():
    regression()
