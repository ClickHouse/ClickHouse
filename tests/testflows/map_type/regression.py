#!/usr/bin/env python3
import os
import sys

from testflows.core import *

append_path(sys.path, "..")

from helpers.cluster import Cluster
from helpers.argparser import argparser
from map_type.requirements import SRS018_ClickHouse_Map_Data_Type

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
    "tests/mapcontains/null key not in map":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/21028")],
    "tests/mapkeys/null key not in map":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/21028")],
    "tests/mapkeys/null key in map":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/21028")],
    "tests/mapcontains/select nullable key":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/21026")],
    "tests/mapkeys/select keys from column":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/21026")],
    "tests/table map select key with value string/LowCardinality:":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/21406")],
    "tests/table map select key with key string/FixedString":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/21406")],
    "tests/table map select key with key string/Nullable":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/21406")],
    "tests/table map select key with key string/Nullable(NULL)":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/21026")],
    "tests/table map select key with key string/LowCardinality:":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/21406")],
    "tests/table map select key with key integer/Int:":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/21032")],
    "tests/table map select key with key integer/UInt256":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/21031")],
    "tests/table map select key with key integer/toNullable":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/21406")],
    "tests/table map select key with key integer/toNullable(NULL)":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/21026")],
    "tests/select map with key integer/Int128":
        [(Fail, "large Int128 as key not supported")],
    "tests/select map with key integer/Int256":
        [(Fail, "large Int256 as key not supported")],
    "tests/select map with key integer/UInt256":
        [(Fail, "large UInt256 as key not supported")],
    "tests/select map with key integer/toNullable":
        [(Fail, "Nullable type as key not supported")],
    "tests/select map with key integer/toNullable(NULL)":
        [(Fail, "Nullable type as key not supported")],
    "tests/select map with key string/Nullable":
        [(Fail, "Nullable type as key not supported")],
    "tests/select map with key string/Nullable(NULL)":
        [(Fail, "Nullable type as key not supported")],
    "tests/table map queries/select map with nullable value":
        [(Fail, "Nullable value not supported")],
    "tests/table map with key integer/toNullable":
        [(Fail, "Nullable type as key not supported")],
    "tests/table map with key integer/toNullable(NULL)":
        [(Fail, "Nullable type as key not supported")],
    "tests/table map with key string/Nullable":
        [(Fail, "Nullable type as key not supported")],
    "tests/table map with key string/Nullable(NULL)":
        [(Fail, "Nullable type as key not supported")],
    "tests/table map with key string/LowCardinality(String)":
        [(Fail, "LowCardinality(String) as key not supported")],
    "tests/table map with key string/LowCardinality(String) cast from String":
        [(Fail, "LowCardinality(String) as key not supported")],
    "tests/table map with key string/LowCardinality(String) for key and value":
        [(Fail, "LowCardinality(String) as key not supported")],
    "tests/table map with key string/LowCardinality(FixedString)":
        [(Fail, "LowCardinality(FixedString) as key not supported")],
    "tests/table map with value string/LowCardinality(String) for key and value":
        [(Fail, "LowCardinality(String) as key not supported")],
    # JSON related
    "tests/table map with duplicated keys/Map(Int64, String))":
        [(Fail, "new bug due to JSON changes")],
    "tests/table map with key integer/UInt64":
        [(Fail, "new bug due to JSON changes")],
    "tests/table map with value integer/UInt64":
        [(Fail, "new bug due to JSON changes")]
}

xflags = {
}

@TestModule
@ArgumentParser(argparser)
@XFails(xfails)
@XFlags(xflags)
@Name("map type")
@Specifications(
    SRS018_ClickHouse_Map_Data_Type
)
def regression(self, local, clickhouse_binary_path, stress=None, parallel=None):
    """Map type regression.
    """
    top().terminating = False
    nodes = {
        "clickhouse":
            ("clickhouse1", "clickhouse2", "clickhouse3")
    }

    if stress is not None:
        self.context.stress = stress
    if parallel is not None:
        self.context.parallel = parallel

    with Cluster(local, clickhouse_binary_path, nodes=nodes,
            docker_compose_project_dir=os.path.join(current_dir(), "map_type_env")) as cluster:
        self.context.cluster = cluster

        Feature(run=load("map_type.tests.feature", "feature"))

if main():
    regression()
