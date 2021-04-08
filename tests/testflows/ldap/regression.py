#!/usr/bin/env python3
import sys
from testflows.core import *

append_path(sys.path, "..")

from helpers.argparser import argparser

@TestModule
@Name("ldap")
@ArgumentParser(argparser)
def regression(self, local, clickhouse_binary_path, parallel=None, stress=None):
    """ClickHouse LDAP integration regression module.
    """
    args = {"local": local, "clickhouse_binary_path": clickhouse_binary_path}

    Feature(test=load("ldap.authentication.regression", "regression"))(**args)
    Feature(test=load("ldap.external_user_directory.regression", "regression"))(**args)

if main():
    regression()