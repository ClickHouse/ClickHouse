#!/usr/bin/env python3
import sys

from testflows.core import *

append_path(sys.path, "..")

from helpers.cluster import Cluster
from helpers.argparser import argparser
from map_type.requirements import QA_SRS018_ClickHouse_Map_Data_Type 

xfails = {
    "tests/table map with key integer/Int:":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/21032")],
    "tests/table map with value integer/Int:":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/21032")],
    "tests/table map with key integer/UInt256":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/21031")],
    "tests/table map with value integer/UInt256":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/21031")],
    "tests/select map with key integer/Int64":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/21030")],
    "tests/select map with value integer/Int64":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/21030")],
    "tests/cast tuple of two arrays to map/string -> int":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/21029")],
    "tests/mapcontains/null key in map":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/21028")],
    "tests/mapcontains/select nullable key":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/21026")]
}

xflags = {
}

@TestModule
@ArgumentParser(argparser)
@XFails(xfails)
@XFlags(xflags)
@Name("map type")
@Specifications(
    QA_SRS018_ClickHouse_Map_Data_Type
)
def regression(self, local, clickhouse_binary_path, stress=None, parallel=None):
    """Map type regression.
    """
    nodes = {
        "clickhouse":
            ("clickhouse1", "clickhouse2", "clickhouse3")
    }
    with Cluster(local, clickhouse_binary_path, nodes=nodes) as cluster:
        self.context.cluster = cluster
        self.context.stress = stress

        if parallel is not None:
            self.context.parallel = parallel

        Feature(run=load("map_type.tests.feature", "feature"))

if main():
    regression()
