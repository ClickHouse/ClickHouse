#!/usr/bin/env python3
import os
import sys
from testflows.core import *

append_path(sys.path, "..")

from helpers.cluster import Cluster
from helpers.argparser import argparser

@TestFeature
@Name("example")
@ArgumentParser(argparser)
def regression(self, local, clickhouse_binary_path, stress=None, parallel=None):
    """Simple example of how you can use TestFlows to test ClickHouse.
    """
    top().terminating = False
    nodes = {
        "clickhouse": ("clickhouse1",),
    }

    if stress is not None:
        self.context.stress = stress
    if parallel is not None:
        self.context.parallel = parallel

    with Cluster(local, clickhouse_binary_path, nodes=nodes,
            docker_compose_project_dir=os.path.join(current_dir(), "example_env")) as cluster:
        self.context.cluster = cluster

        Scenario(run=load("example.tests.example", "scenario"))

if main():
    regression()
