#!/usr/bin/env python3
import sys
from testflows.core import *

append_path(sys.path, "..")

from helpers.cluster import Cluster
from helpers.argparser import argparser
from datetime64_extended_range.requirements import *
from datetime64_extended_range.common import *

# cross-outs
# https://github.com/ClickHouse/ClickHouse/issues/16581#issuecomment-804360350: 128 and 256-bit types are not supported for now
# "https://github.com/ClickHouse/ClickHouse/issues/17079#issuecomment-783396589" : leap seconds unsupported

xfails = {
    "type conversion/to int 8 16 32 64 128 256/:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/16581#issuecomment-804360350")],
    "type conversion/to uint 8 16 32 64 256/:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/16581#issuecomment-804360350")],
    "non existent time/leap seconds/:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/17079#issuecomment-783396589")],

}


@TestModule
@Name("datetime64 extended range")
@ArgumentParser(argparser)
@Specifications(
    QA_SRS010_ClickHouse_DateTime64_Extended_Range
)
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange("1.0"),
)
@XFails(xfails)
def regression(self, local, clickhouse_binary_path, parallel=False, stress=False):
    """ClickHouse DateTime64 Extended Range regression module.
    """
    nodes = {
        "clickhouse": ("clickhouse1", "clickhouse2", "clickhouse3"),
    }

    with Cluster(local, clickhouse_binary_path, nodes=nodes) as cluster:
        self.context.cluster = cluster
        self.context.parallel = parallel
        self.context.stress = stress

        Scenario(run=load("datetime64_extended_range.tests.generic", "generic"), flags=TE)
        Scenario(run=load("datetime64_extended_range.tests.non_existent_time", "feature"), flags=TE)
        Scenario(run=load("datetime64_extended_range.tests.reference_times", "reference_times"), flags=TE)
        Scenario(run=load("datetime64_extended_range.tests.date_time_functions", "date_time_funcs"), flags=TE)
        Scenario(run=load("datetime64_extended_range.tests.type_conversion", "type_conversion"), flags=TE)

if main():
    regression()
