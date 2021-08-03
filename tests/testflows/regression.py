#!/usr/bin/env python3
import sys
from testflows.core import *

append_path(sys.path, ".")

from helpers.common import Pool, join, run_scenario
from helpers.argparser import argparser

@TestModule
@Name("clickhouse")
@ArgumentParser(argparser)
def regression(self, local, clickhouse_binary_path, stress=None, parallel=None):
    """ClickHouse regression.
    """
    top().terminating = False
    args = {"local": local, "clickhouse_binary_path": clickhouse_binary_path, "stress": stress, "parallel": parallel}

    self.context.stress = stress
    self.context.parallel = parallel

    tasks = []
    with Pool(8) as pool:
        try:
            run_scenario(pool, tasks, Feature(test=load("example.regression", "regression")), args)
            #run_scenario(pool, tasks, Feature(test=load("ldap.regression", "regression")), args)
            run_scenario(pool, tasks, Feature(test=load("rbac.regression", "regression")), args)
            run_scenario(pool, tasks, Feature(test=load("aes_encryption.regression", "regression")), args)
            run_scenario(pool, tasks, Feature(test=load("map_type.regression", "regression")), args)
            run_scenario(pool, tasks, Feature(test=load("window_functions.regression", "regression")), args)
            run_scenario(pool, tasks, Feature(test=load("datetime64_extended_range.regression", "regression")), args)
            #run_scenario(pool, tasks, Feature(test=load("kerberos.regression", "regression")), args)
            run_scenario(pool, tasks, Feature(test=load("extended_precision_data_types.regression", "regression")), args)
        finally:
            join(tasks)

if main():
    regression()
