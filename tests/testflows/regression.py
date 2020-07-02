#!/usr/bin/env python3
import sys
from testflows.core import *

append_path(sys.path, "."), 

from helpers.argparser import argparser

@TestModule
@Name("clickhouse")
@ArgumentParser(argparser)
def regression(self, local, clickhouse_binary_path):
    """ClickHouse functional regression module.
    """
    Feature(test=load("ldap.regression", "regression"))(
        local=local, clickhouse_binary_path=clickhouse_binary_path)

if main():
    regression()
