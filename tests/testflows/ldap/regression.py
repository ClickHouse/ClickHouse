#!/usr/bin/env python3
import sys
from testflows.core import *

append_path(sys.path, "..") 

from helpers.cluster import Cluster
from helpers.argparser import argparser
from ldap.requirements import *

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
     [(Fail, "can't get it to work")],
    "connection protocols/tls minimum protocol version/:":
     [(Fail, "can't get it to work")]
}

@TestFeature
@Name("ldap authentication")
@ArgumentParser(argparser)
@Requirements(
    RQ_SRS_007_LDAP_Authentication("1.0")
)
@XFails(xfails)
def regression(self, local, clickhouse_binary_path):
    """ClickHouse integration with LDAP regression module.
    """
    nodes = {
        "clickhouse": ("clickhouse1", "clickhouse2", "clickhouse3"),
    }
 
    with Cluster(local, clickhouse_binary_path, nodes=nodes) as cluster:
        self.context.cluster = cluster

        Scenario(run=load("ldap.tests.sanity", "scenario"))
        Scenario(run=load("ldap.tests.multiple_servers", "scenario"))
        Feature(run=load("ldap.tests.connections", "feature"))
        Feature(run=load("ldap.tests.server_config", "feature"))
        Feature(run=load("ldap.tests.user_config", "feature"))
        Feature(run=load("ldap.tests.authentications", "feature"))

if main():
    regression()
