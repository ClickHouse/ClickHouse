#!/usr/bin/env python3
import sys
from testflows.core import *

append_path(sys.path, "..", "..")

from helpers.cluster import Cluster
from helpers.argparser import argparser
from ldap.authentication.requirements import *

# Cross-outs of known fails
xfails = {
    "connection protocols/tls/tls_require_cert='try'":
     [(Fail, "can't be tested with self-signed certificates")],
    "connection protocols/tls/tls_require_cert='demand'":
     [(Fail, "can't be tested with self-signed certificates")],
    "connection protocols/starttls/tls_require_cert='try'":
     [(Fail, "can't be tested with self-signed certificates")],
    "connection protocols/starttls/tls_require_cert='demand'":
     [(Fail, "can't be tested with self-signed certificates")],
    "connection protocols/tls require cert default demand":
     [(Fail, "can't be tested with self-signed certificates")],
    "connection protocols/starttls with custom port":
     [(Fail, "it seems that starttls is not enabled by default on custom plain-text ports in LDAP server")],
    "connection protocols/tls cipher suite":
     [(Fail, "can't get it to work")]
}

@TestFeature
@Name("authentication")
@ArgumentParser(argparser)
@Requirements(
    RQ_SRS_007_LDAP_Authentication("1.0")
)
@XFails(xfails)
def regression(self, local, clickhouse_binary_path, stress=None, parallel=None):
    """ClickHouse integration with LDAP regression module.
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

        Scenario(run=load("ldap.authentication.tests.sanity", "scenario"))
        Scenario(run=load("ldap.authentication.tests.multiple_servers", "scenario"))
        Feature(run=load("ldap.authentication.tests.connections", "feature"))
        Feature(run=load("ldap.authentication.tests.server_config", "feature"))
        Feature(run=load("ldap.authentication.tests.user_config", "feature"))
        Feature(run=load("ldap.authentication.tests.authentications", "feature"))

if main():
    regression()
