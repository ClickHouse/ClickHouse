#!/usr/bin/env python3
import sys
from testflows.core import *

append_path(sys.path, ".")

from helpers.argparser import argparser

@TestModule
@Name("clickhouse")
@ArgumentParser(argparser)
def regression(self, local, clickhouse_binary_path, stress=None, parallel=None):
    """ClickHouse regression.
    """
    args = {"local": local, "clickhouse_binary_path": clickhouse_binary_path, "stress": stress, "parallel": parallel}

    Feature(test=load("example.regression", "regression"))(**args)
    Feature(test=load("ldap.regression", "regression"))(**args)
    Feature(test=load("rbac.regression", "regression"))(**args)
    Feature(test=load("aes_encryption.regression", "regression"))(**args)

if main():
    regression()
