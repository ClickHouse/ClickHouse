#!/usr/bin/env python3
import sys
from testflows.core import *

append_path(sys.path, "..")

from helpers.cluster import Cluster
from helpers.argparser import argparser
from aes_encryption.requirements import *

xfails = {
    # encrypt
    "encrypt/invalid key or iv length for mode/mode=\"'aes-???-gcm'\", key_len=??, iv_len=12, aad=True/iv is too short":
     [(Fail, "known issue")],
    "encrypt/invalid key or iv length for mode/mode=\"'aes-???-gcm'\", key_len=??, iv_len=12, aad=True/iv is too long":
     [(Fail, "known issue")],
    # encrypt_mysql
    "encrypt_mysql/key or iv length for mode/mode=\"'aes-???-ecb'\", key_len=??, iv_len=None":
     [(Fail, "https://altinity.atlassian.net/browse/CH-190")],
    "encrypt_mysql/invalid parameters/iv not valid for mode":
     [(Fail, "https://altinity.atlassian.net/browse/CH-190")],
    "encrypt_mysql/invalid parameters/no parameters":
     [(Fail, "https://altinity.atlassian.net/browse/CH-191")],
    # decrypt_mysql
    "decrypt_mysql/key or iv length for mode/mode=\"'aes-???-ecb'\", key_len=??, iv_len=None:":
     [(Fail, "https://altinity.atlassian.net/browse/CH-190")],
    # compatibility
    "compatibility/insert/encrypt using materialized view/:":
     [(Fail, "https://altinity.atlassian.net/browse/CH-193")],
    "compatibility/insert/decrypt using materialized view/:":
     [(Error, "https://altinity.atlassian.net/browse/CH-193")],
    "compatibility/insert/aes encrypt mysql using materialized view/:":
     [(Fail, "https://altinity.atlassian.net/browse/CH-193")],
    "compatibility/insert/aes decrypt mysql using materialized view/:":
     [(Error, "https://altinity.atlassian.net/browse/CH-193")],
    "compatibility/select/decrypt unique":
     [(Fail, "https://altinity.atlassian.net/browse/CH-193")],
    "compatibility/mysql/:engine/decrypt/mysql_datatype='TEXT'/:":
     [(Fail, "https://altinity.atlassian.net/browse/CH-194")],
    "compatibility/mysql/:engine/decrypt/mysql_datatype='VARCHAR(100)'/:":
     [(Fail, "https://altinity.atlassian.net/browse/CH-194")],
    "compatibility/mysql/:engine/encrypt/mysql_datatype='TEXT'/:":
     [(Fail, "https://altinity.atlassian.net/browse/CH-194")],
    "compatibility/mysql/:engine/encrypt/mysql_datatype='VARCHAR(100)'/:":
     [(Fail, "https://altinity.atlassian.net/browse/CH-194")]
}

@TestFeature
@Name("aes encryption")
@ArgumentParser(argparser)
@Requirements(
    RQ_SRS008_AES_Functions("1.0"),
    RQ_SRS008_AES_Functions_DifferentModes("1.0")
)
@XFails(xfails)
def regression(self, local, clickhouse_binary_path):
    """ClickHouse AES encryption functions regression module.
    """
    nodes = {
        "clickhouse": ("clickhouse1", "clickhouse2", "clickhouse3"),
    }

    with Cluster(local, clickhouse_binary_path, nodes=nodes) as cluster:
        self.context.cluster = cluster

        Feature(run=load("aes_encryption.tests.encrypt", "feature"), flags=TE)
        Feature(run=load("aes_encryption.tests.decrypt", "feature"), flags=TE)
        Feature(run=load("aes_encryption.tests.encrypt_mysql", "feature"), flags=TE)
        Feature(run=load("aes_encryption.tests.decrypt_mysql", "feature"), flags=TE)
        Feature(run=load("aes_encryption.tests.compatibility.feature", "feature"), flags=TE)

if main():
    regression()
