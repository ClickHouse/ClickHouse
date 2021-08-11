#!/usr/bin/env python3
import os
import sys

from testflows.core import *

append_path(sys.path, "..")

from helpers.cluster import Cluster
from helpers.argparser import argparser
from ssl_server.requirements import SRS017_ClickHouse_Security_SSL_Server

xfails = {
}

xflags = {
}

@TestModule
@ArgumentParser(argparser)
@XFails(xfails)
@XFlags(xflags)
@Name("ssl server")
@Specifications(
    SRS017_ClickHouse_Security_SSL_Server
)
def regression(self, local, clickhouse_binary_path, stress=None):
    """ClickHouse security SSL server regression.
    """
    nodes = {
        "clickhouse":
            ("clickhouse1", "clickhouse2", "clickhouse3")
    }

    if stress is not None:
        self.context.stress = stress

    with Cluster(local, clickhouse_binary_path, nodes=nodes,
            docker_compose_project_dir=os.path.join(current_dir(), "ssl_server_env")) as cluster:
        self.context.cluster = cluster

        Feature(run=load("ssl_server.tests.sanity", "feature"))
        Feature(run=load("ssl_server.tests.dynamic_ssl_context", "feature"))

if main():
    regression()
