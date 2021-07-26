import sys
from testflows.core import *

append_path(sys.path, "..")

from helpers.cluster import Cluster
from helpers.argparser import argparser
from kerberos.requirements.requirements import *

xfails = {
}


@TestModule
@Name("kerberos")
@ArgumentParser(argparser)
@Requirements(
    RQ_SRS_016_Kerberos("1.0")
)
@XFails(xfails)
def regression(self, local, clickhouse_binary_path, stress=None, parallel=None):
    """ClickHouse Kerberos authentication test regression module.
    """
    nodes = {
        "clickhouse": ("clickhouse1", "clickhouse2", "clickhouse3"),
        "kerberos": ("kerberos", ),
    }

    with Cluster(local, clickhouse_binary_path, nodes=nodes) as cluster:
        self.context.cluster = cluster

        Feature(run=load("kerberos.tests.generic", "generic"), flags=TE)
        Feature(run=load("kerberos.tests.config", "config"), flags=TE)
        Feature(run=load("kerberos.tests.parallel", "parallel"), flags=TE)


if main():
    regression()
