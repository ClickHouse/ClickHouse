#!/usr/bin/env python3
import sys

from testflows.core import *

append_path(sys.path, "..")

from helpers.cluster import Cluster
from helpers.argparser import argparser
from window_functions.requirements import SRS019_ClickHouse_Window_Functions, RQ_SRS_019_ClickHouse_WindowFunctions

xfails = {
    "tests/:/frame clause/range frame/between expr following and expr following without order by error":
        [(Fail, "invalid error message")],
    "tests/:/frame clause/range frame/between expr following and expr preceding without order by error":
        [(Fail, "invalid error message")],
    "tests/:/frame clause/range frame/between expr following and current row without order by error":
        [(Fail, "invalid error message")],
    "tests/:/frame clause/range frame/between expr following and current row zero special case":
        [(Fail, "known bug")],
    "tests/:/frame clause/range frame/between expr following and expr preceding with order by zero special case":
        [(Fail, "known bug")],
    "tests/:/funcs/lag/anyOrNull with column value as offset":
        [(Fail, "column values are not supported as offset")],
    "tests/:/funcs/lead/subquery as offset":
        [(Fail, "subquery is not supported as offset")],
    "tests/:/frame clause/range frame/between current row and unbounded following modifying named window":
        [(Fail, "range with named window is not supported")],
    "tests/:/frame clause/range overflow/negative overflow with Int16":
        [(Fail, "exception on conversion")],
    "tests/:/frame clause/range overflow/positive overflow with Int16":
        [(Fail, "exception on conversion")],
    "tests/:/misc/subquery expr preceding":
        [(Fail, "subquery is not supported as offset")],
    "tests/:/frame clause/range errors/error negative preceding offset":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/22442")],
    "tests/:/frame clause/range errors/error negative following offset":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/22442")],
    "tests/:/misc/window functions in select expression":
        [(Fail, "not supported, https://github.com/ClickHouse/ClickHouse/issues/19857")],
    "tests/:/misc/window functions in subquery":
        [(Fail, "not supported, https://github.com/ClickHouse/ClickHouse/issues/19857")],
    "tests/:/frame clause/range frame/order by decimal":
        [(Fail, "Exception: The RANGE OFFSET frame for 'DB::ColumnDecimal<DB::Decimal<long> >' ORDER BY column is not implemented")],
    "tests/:/frame clause/range frame/with nulls":
        [(Fail, "DB::Exception: The RANGE OFFSET frame for 'DB::ColumnNullable' ORDER BY column is not implemented")],
    "tests/:/aggregate funcs/aggregate funcs over rows frame/func='mannWhitneyUTest(salary, 1)'":
        [(Fail, "need to investigate")],
    "tests/:/aggregate funcs/aggregate funcs over rows frame/func='rankCorr(salary, 0.5)'":
        [(Fail, "need to investigate")],
    "tests/distributed/misc/query with order by and one window":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/23902")],
    "tests/distributed/over clause/empty named window":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/23902")],
    "tests/distributed/over clause/empty":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/23902")],
    "tests/distributed/over clause/adhoc window":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/23902")],
    "tests/distributed/frame clause/range datetime/:":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/23902")],
    "tests/distributed/frame clause/range frame/between expr preceding and expr following with partition by same column twice":
        [(Fail, "https://github.com/ClickHouse/ClickHouse/issues/23902")]
}

xflags = {
}

@TestModule
@ArgumentParser(argparser)
@XFails(xfails)
@XFlags(xflags)
@Name("window functions")
@Specifications(
    SRS019_ClickHouse_Window_Functions
)
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions("1.0")
)
def regression(self, local, clickhouse_binary_path, stress=None, parallel=None):
    """Window functions regression.
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

        Feature(run=load("window_functions.tests.feature", "feature"), flags=TE)

if main():
    regression()
