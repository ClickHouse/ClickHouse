#!/usr/bin/env python3
import sys
from testflows.core import *

append_path(sys.path, "..")

from helpers.argparser import argparser


@TestModule
@Name("ldap")
@ArgumentParser(argparser)
def regression(
    self, local, clickhouse_binary_path, clickhouse_version=None, stress=None
):
    """ClickHouse LDAP integration regression module."""
    args = {
        "local": local,
        "clickhouse_binary_path": clickhouse_binary_path,
        "clickhouse_version": clickhouse_version,
    }

    self.context.clickhouse_version = clickhouse_version

    if stress is not None:
        self.context.stress = stress

    with Pool(3) as pool:
        try:
            Feature(
                test=load("ldap.authentication.regression", "regression"),
                parallel=True,
                executor=pool,
            )(**args)
            Feature(
                test=load("ldap.external_user_directory.regression", "regression"),
                parallel=True,
                executor=pool,
            )(**args)
            Feature(
                test=load("ldap.role_mapping.regression", "regression"),
                parallel=True,
                executor=pool,
            )(**args)
        finally:
            join()


if main():
    regression()
