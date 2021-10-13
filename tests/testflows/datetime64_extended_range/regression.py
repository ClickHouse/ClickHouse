#!/usr/bin/env python3
import os
import sys
from testflows.core import *

append_path(sys.path, "..")

from helpers.cluster import Cluster
from helpers.argparser import argparser
from datetime64_extended_range.requirements import *
from datetime64_extended_range.common import *

# cross-outs
# https://github.com/ClickHouse/ClickHouse/issues/16581#issuecomment-804360350: 128 and 256-bit types are not supported for now
# https://github.com/ClickHouse/ClickHouse/issues/17079#issuecomment-783396589 : leap seconds unsupported
# https://github.com/ClickHouse/ClickHouse/issues/22824 : dateDiff not working woth dt64
# https://github.com/ClickHouse/ClickHouse/issues/22852 : formatDateTime wrong value
# https://github.com/ClickHouse/ClickHouse/issues/22854 : timeSlot(), toMonday() wrong when out of normal
# https://github.com/ClickHouse/ClickHouse/issues/16260 : timeSlots(), dateDiff() not working with DT64
# https://github.com/ClickHouse/ClickHouse/issues/22927#issuecomment-816574952 : toRelative...Num() wrong when out of normal range
# https://github.com/ClickHouse/ClickHouse/issues/22928 : toStartOf...() wrong when out of normal range
# https://github.com/ClickHouse/ClickHouse/issues/22929 : toUnixTimestamp() exception when out of normal
# https://github.com/ClickHouse/ClickHouse/issues/22930 : toWeek()
# https://github.com/ClickHouse/ClickHouse/issues/22948 : toYearWeek()
# https://github.com/ClickHouse/ClickHouse/issues/22959 : toUnixTimestamp64*() wrong fractal seconds treatment

# For `reference times` test it is unclear how to evaluate correctness - majority of test cases are correct, and ONLY
# Juba and Monrovia timezones are damaged - probably, due to wrong DST shifts lookup tables

xfails = {
    "type conversion/to int 8 16 32 64 128 256/:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/16581#issuecomment-804360350")],
    "type conversion/to uint 8 16 32 64 256/:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/16581#issuecomment-804360350")],
    "non existent time/leap seconds/:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/17079#issuecomment-783396589")],
    "date time funcs/date diff/:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/22824")],
    "date time funcs/format date time/:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/22852")],
    "date time funcs/time slot/:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/22854")],
    "date time funcs/to monday/:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/22854")],
    "date time funcs/time slots/:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/16260")],
    "date time funcs/to relative :/:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/22927#issuecomment-816574952")],
    "date time funcs/to start of :/:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/22928")],
    "date time funcs/to unix timestamp/:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/22929")],
    "date time funcs/to week/:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/22930")],
    "date time funcs/to year week/:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/22948")],
    "type conversion/to unix timestamp64 */:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/22959")],
    "type conversion/from unix timestamp64 */:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/22959")],
    "type conversion/to int 8 16 32 64 128 256/:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/16581#issuecomment-804360350")],
    "reference times/:": [(Fail, "check procedure unclear")],
    # need to investigate
    "type conversion/to datetime/cast=True": [(Fail, "need to investigate")],
    "date time funcs/today": [(Fail, "need to investigate")]
}


@TestModule
@Name("datetime64 extended range")
@ArgumentParser(argparser)
@Specifications(
    SRS_010_ClickHouse_DateTime64_Extended_Range
)
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange("1.0"),
)
@XFails(xfails)
def regression(self, local, clickhouse_binary_path, parallel=False, stress=False):
    """ClickHouse DateTime64 Extended Range regression module.
    """
    top().terminating = False
    nodes = {
        "clickhouse": ("clickhouse1", "clickhouse2", "clickhouse3"),
    }

    if stress is not None:
        self.context.stress = stress
    if parallel is not None:
        self.context.parallel = parallel

    with Cluster(local, clickhouse_binary_path, nodes=nodes,
            docker_compose_project_dir=os.path.join(current_dir(), "datetime64_extended_range_env")) as cluster:
        self.context.cluster = cluster

        tasks = []
        with Pool(2) as pool:
            try:
                run_scenario(pool, tasks, Scenario(test=load("datetime64_extended_range.tests.generic", "generic")))
                run_scenario(pool, tasks, Scenario(test=load("datetime64_extended_range.tests.non_existent_time", "feature")))
                run_scenario(pool, tasks, Scenario(test=load("datetime64_extended_range.tests.reference_times", "reference_times")))
                run_scenario(pool, tasks, Scenario(test=load("datetime64_extended_range.tests.date_time_functions", "date_time_funcs")))
                run_scenario(pool, tasks, Scenario(test=load("datetime64_extended_range.tests.type_conversion", "type_conversion")))
            finally:
                join(tasks)

if main():
    regression()
