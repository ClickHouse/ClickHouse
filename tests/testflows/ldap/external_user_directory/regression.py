#!/usr/bin/env python3
import sys
from testflows.core import *

append_path(sys.path, "..", "..")

from helpers.cluster import Cluster
from helpers.argparser import argparser
from ldap.external_user_directory.requirements import *

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
@Name("external user directory")
@ArgumentParser(argparser)
@Requirements(
    RQ_SRS_009_LDAP_ExternalUserDirectory_Authentication("1.0")
)
@XFails(xfails)
def regression(self, local, clickhouse_binary_path, stress=None, parallel=None):
    """ClickHouse LDAP external user directory regression module.
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
        Scenario(run=load("ldap.external_user_directory.tests.simple", "scenario"))
        Feature(run=load("ldap.external_user_directory.tests.restart", "feature"))
        Feature(run=load("ldap.external_user_directory.tests.server_config", "feature"))
        Feature(run=load("ldap.external_user_directory.tests.external_user_directory_config", "feature"))
        Feature(run=load("ldap.external_user_directory.tests.connections", "feature"))
        Feature(run=load("ldap.external_user_directory.tests.authentications", "feature"))
        Feature(run=load("ldap.external_user_directory.tests.roles", "feature"))

if main():
    regression()
