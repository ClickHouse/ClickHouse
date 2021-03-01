#!/usr/bin/env python3
import sys
from testflows.core import *

append_path(sys.path, "..", "..")

from helpers.cluster import Cluster
from helpers.argparser import argparser
from ldap.role_mapping.requirements import *

# Cross-outs of known fails
xfails = {
   "mapping/roles removed and added in parallel":
       [(Fail, "known bug")]
}

@TestFeature
@Name("role mapping")
@ArgumentParser(argparser)
@Specifications(
    SRS_014_ClickHouse_LDAP_Role_Mapping
)
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping("1.0")
)
@XFails(xfails)
def regression(self, local, clickhouse_binary_path, stress=None, parallel=None):
    """ClickHouse LDAP role mapping regression module.
    """
    nodes = {
        "clickhouse": ("clickhouse1", "clickhouse2", "clickhouse3"),
    }

    with Cluster(local, clickhouse_binary_path, nodes=nodes) as cluster:
        self.context.cluster = cluster
        
        if stress is not None or not hasattr(self.context, "stress"):
            self.context.stress = stress
        if parallel is not None or not hasattr(self.context, "parallel"):
            self.context.parallel = parallel

        Scenario(run=load("ldap.authentication.tests.sanity", "scenario"), name="ldap sanity")
        Feature(run=load("ldap.role_mapping.tests.server_config", "feature"))
        Feature(run=load("ldap.role_mapping.tests.mapping", "feature"))

if main():
    regression()
