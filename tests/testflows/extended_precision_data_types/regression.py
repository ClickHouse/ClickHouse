#!/usr/bin/env python3
import os
import sys

from testflows.core import *

append_path(sys.path, "..")

from helpers.cluster import Cluster
from helpers.argparser import argparser
from extended_precision_data_types.requirements import *

xfails = {}

xflags = {}


@TestModule
@ArgumentParser(argparser)
@XFails(xfails)
@XFlags(xflags)
@Name("extended precision data types")
@Specifications(SRS020_ClickHouse_Extended_Precision_Data_Types)
@Requirements(
    RQ_SRS_020_ClickHouse_Extended_Precision("1.0"),
)
def regression(
    self, local, clickhouse_binary_path, clickhouse_version=None, stress=None
):
    """Extended precision data type regression."""
    nodes = {"clickhouse": ("clickhouse1",)}
    if stress is not None:
        self.context.stress = stress
    self.context.clickhouse_version = clickhouse_version

    with Cluster(
        local,
        clickhouse_binary_path,
        nodes=nodes,
        docker_compose_project_dir=os.path.join(
            current_dir(), "extended-precision-data-type_env"
        ),
    ) as cluster:

        self.context.cluster = cluster

        Feature(run=load("extended_precision_data_types.tests.feature", "feature"))


if main():
    regression()
