#!/usr/bin/env python3
import sys
from testflows.core import *

append_path(sys.path, "..") 

from helpers.cluster import Cluster
from helpers.argparser import argparser

@TestFeature
@Name("example")
@ArgumentParser(argparser)
def regression(self, local, clickhouse_binary_path):
    """Simple example of how you can use TestFlows to test ClickHouse.
    """
    nodes = {
        "clickhouse": ("clickhouse1",),
    }
 
    with Cluster(local, clickhouse_binary_path, nodes=nodes) as cluster:
        self.context.cluster = cluster

        Scenario(run=load("example.tests.example", "scenario"))

if main():
    regression()
