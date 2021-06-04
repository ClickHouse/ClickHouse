#!/usr/bin/env python3
import sys
from testflows.core import *

append_path(sys.path, "..")

from helpers.common import Pool, join, run_scenario
from helpers.argparser import argparser

@TestModule
@Name("ldap")
@ArgumentParser(argparser)
def regression(self, local, clickhouse_binary_path, parallel=None, stress=None):
    """ClickHouse LDAP integration regression module.
    """
    top().terminating = False
    args = {"local": local, "clickhouse_binary_path": clickhouse_binary_path}

    if stress is not None:
        self.context.stress = stress
    if parallel is not None:
        self.context.parallel = parallel

    tasks = []
    with Pool(3) as pool:
        try:
            run_scenario(pool, tasks, Feature(test=load("ldap.authentication.regression", "regression")), args)
            run_scenario(pool, tasks, Feature(test=load("ldap.external_user_directory.regression", "regression")), args)
            run_scenario(pool, tasks, Feature(test=load("ldap.role_mapping.regression", "regression")), args)
        finally:
            join(tasks)

if main():
    regression()
