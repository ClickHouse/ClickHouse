from contextlib import contextmanager

from testflows.core import *

import subprocess

@TestFeature
@Name("settings profile bughunt")
def feature(self, node="clickhouse1"):

    node = self.context.cluster.node(node)

    try:
        with Given("I have a table"):
            node.query("DROP TABLE IF EXISTS default.foo")
            node.query("CREATE TABLE default.foo (x UInt64) Engine=Memory")

        with And("I put data into the table"):
            node.query("INSERT INTO default.foo(x) VALUES (1)")

        with And("I have a user"):
            node.query("CREATE USER OR REPLACE user0")

        with And("I check number of entries"):
            node.query("SELECT count(*) FROM default.foo")

        with Scenario("I create a condition settings profile and assign it to user and check the entries again"):
            node.query("CREATE ROW POLICY policy0 ON default.foo AS PERMISSIVE USING x <> 1")
            node.query("SELECT count(*) FROM default.foo", message ="1")

    finally:
        with Finally("I drop the table, user, and profile"):
            node.query("DROP TABLE IF EXISTS default.foo")
            node.query("DROP USER IF EXISTS user0")
            node.query("DROP SETTINGS PROFILE IF EXISTS profile0")
            subprocess.call("rm -rf _instances", shell=True)
