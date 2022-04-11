#!/usr/bin/env python3
import sys
from testflows.core import *

append_path(sys.path, ".")

from helpers.argparser import argparser as argparser_base

def argparser(parser):
    argparser_base(parser)
    parser.add_argument(
        "--use-alter-delete",
        action="store_true",
        help="Use alter delete instead of lightweight delete.",
    )


@TestModule
@Name("clickhouse")
@ArgumentParser(argparser)
def regression(
    self, local, clickhouse_binary_path, use_alter_delete=False, clickhouse_version=None, stress=None
):
    """ClickHouse regression."""
    args = {
        "local": local,
        "clickhouse_binary_path": clickhouse_binary_path,
        "clickhouse_version": clickhouse_version,
        "stress": stress,
    }
   
    args_for_lightweight_delete = {
        "local": local,
        "clickhouse_binary_path": clickhouse_binary_path,
        "clickhouse_version": clickhouse_version,
        "use_alter_delete": use_alter_delete,
        "stress": stress,
    }

    self.context.stress = stress
    self.context.clickhouse_version = clickhouse_version

    with Pool(9) as pool:
        try:
            Feature(
                test=load("example.regression", "regression"),
                parallel=True,
                executor=pool,
            )(**args)
            Feature(
                test=load("ldap.regression", "regression"), parallel=True, executor=pool
            )(**args)
            Feature(
                test=load("rbac.regression", "regression"), parallel=True, executor=pool
            )(**args)
            Feature(
                test=load("lightweight_delete.regression", "regression"), parallel=True, executor=pool
            )(**args_for_lightweight_delete)
            Feature(
                test=load("aes_encryption.regression", "regression"),
                parallel=True,
                executor=pool,
            )(
                **args
            )  # TODO: fix it!
            # Feature(test=load("map_type.regression", "regression"), parallel=True, executor=pool)(**args) # TODO: fix it!
            Feature(
                test=load("window_functions.regression", "regression"),
                parallel=True,
                executor=pool,
            )(
                **args
            )  # TODO: fix it!
            Feature(
                test=load("datetime64_extended_range.regression", "regression"),
                parallel=True,
                executor=pool,
            )(**args)
            Feature(
                test=load("kerberos.regression", "regression"),
                parallel=True,
                executor=pool,
            )(**args)
            Feature(
                test=load("extended_precision_data_types.regression", "regression"),
                parallel=True,
                executor=pool,
            )(
                **args
            )  # TODO: fix it!
        finally:
            join()


if main():
    regression()
