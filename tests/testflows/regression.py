#!/usr/bin/env python3
import sys
from testflows.core import *

append_path(sys.path, ".")

from helpers.argparser import argparser

@TestModule
@Name("clickhouse")
@ArgumentParser(argparser)
def regression(self, local, clickhouse_binary_path, stress=None):
    """ClickHouse regression.
    """
    args = {"local": local, "clickhouse_binary_path": clickhouse_binary_path, "stress": stress}

    self.context.stress = stress

    with Pool(8) as pool:
        try:
            Feature(test=load("example.regression", "regression"), parallel=True, executor=pool)(**args)
            # run_scenario(pool, tasks, Feature(test=load("ldap.regression", "regression")), args)
            # run_scenario(pool, tasks, Feature(test=load("rbac.regression", "regression")), args)
            Feature(test=load("aes_encryption.regression", "regression"), parallel=True, executor=pool)(**args)
            # Feature(test=load("map_type.regression", "regression"), parallel=True, executor=pool)(**args)
            Feature(test=load("window_functions.regression", "regression"), parallel=True, executor=pool)(**args)
            Feature(test=load("datetime64_extended_range.regression", "regression"), parallel=True, executor=pool)(**args)
            Feature(test=load("kerberos.regression", "regression"), parallel=True, executor=pool)(**args)
            Feature(test=load("extended_precision_data_types.regression", "regression"), parallel=True, executor=pool)(**args)
        finally:
            join()

if main():
    regression()
