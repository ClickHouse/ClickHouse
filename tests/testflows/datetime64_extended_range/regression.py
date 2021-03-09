#!/usr/bin/env python3
#  Copyright 2020, Altinity LTD. All Rights Reserved.
#
#  All information contained herein is, and remains the property
#  of Altinity LTD. Any dissemination of this information or
#  reproduction of this material is strictly forbidden unless
#  prior written permission is obtained from Altinity LTD.
#
import sys
from testflows.core import *

append_path(sys.path, "..")

from helpers.cluster import Cluster
from helpers.argparser import argparser
from datetime64_extended_range.requirements import *
from datetime64_extended_range.common import *

# cross-outs
# https://github.com/ClickHouse/ClickHouse/issues/15486 : wrong seconds value prior to 1920

xfails = {
    "date time funcs/to year/:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15784")],
    "date time funcs/to time zone/:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15784")],
    "generic/timezone local below normal range/:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/16222")],
    "generic/timezone local below extended range/:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15486")],
    "generic/timezones support*:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15486")],
    "date time funcs/to quarter/:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15784")],
    "date time funcs/to month/:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15784")],
    "date time funcs/to unix timestamp": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15784")],
    "date time funcs/to day of month": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15784")],
    "date time funcs/to day of week": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15784")],
    "date time funcs/to day of year": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15784")],
    "date time funcs/to hour": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15784")],
    "date time funcs/to minute": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15784")],
    "date time funcs/to second": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15784")],
    "date time funcs/to year": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15784")],
    "date time funcs/to iso year": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15784")],
    "date time funcs/to iso week": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15784")],
    "date time funcs/to monday": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15784")],
    "date time funcs/to relative :": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15784")],
    "date time funcs/to time": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15784")],
    "date time funcs/to start of :": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15784")],
    "date time funcs/to yyyy:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15784")],
    "date time funcs/time slot": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15784")],
    "date time funcs/add :": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15486")],
    "date time funcs/subtract :": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15486")],
    "date time funcs/to week": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15784")],
    "date time funcs/to year week": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15784")],
    "date time funcs/format date time": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15784")],
    "date time funcs/date diff/:": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/16260")],
    "date time funcs/time slots*": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/16260")],
    "type conversion/to datetime64": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15486")],
    "type conversion/to uint 8 16 32 64 256*": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/16581")],
    "type conversion/to int 8 16 32 64 128 256*": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/16581")],
    "type conversion/from unix timestamp64*": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15486")],
    "type conversion/to unix timestamp64*": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15486")],
    "type conversion/to string*": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/15486")],
    "type conversion/to datetime64 from string missing time*": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/17080")],
    "non existent time/dst time zone switch*": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/16924")],
    "non existent time/leap seconds*": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/17079")],
    "reference times": [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/16924")]
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
