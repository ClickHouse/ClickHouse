#!/usr/bin/env python3
import os
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
@Specifications(
    SRS_007_ClickHouse_Authentication_of_Users_via_LDAP
)
@Requirements(
    RQ_SRS_007_LDAP_Authentication("1.0")
)
@XFails(xfails)
def regression(self, local, clickhouse_binary_path, stress=None, parallel=None):
    """ClickHouse integration with LDAP regression module.
    """
    top().terminating = False
    nodes = {
        "clickhouse": ("clickhouse1", "clickhouse2", "clickhouse3"),
    }

    if stress is not None:
        self.context.stress = stress
    if parallel is not None:
        self.context.parallel = parallel

    with Cluster(local, clickhouse_binary_path, nodes=nodes,
            docker_compose_project_dir=os.path.join(current_dir(), "ldap_authentication_env")) as cluster:
        self.context.cluster = cluster

        Scenario(run=load("ldap.authentication.tests.sanity", "scenario"))
        Scenario(run=load("ldap.authentication.tests.multiple_servers", "scenario"))
        Feature(run=load("ldap.authentication.tests.connections", "feature"))
        Feature(run=load("ldap.authentication.tests.server_config", "feature"))
        Feature(run=load("ldap.authentication.tests.user_config", "feature"))
        Feature(run=load("ldap.authentication.tests.authentications", "feature"))

if main():
    regression()
