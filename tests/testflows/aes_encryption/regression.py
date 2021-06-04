#!/usr/bin/env python3
import os
import sys
from testflows.core import *

append_path(sys.path, "..")

from helpers.cluster import Cluster
from helpers.common import Pool, join, run_scenario
from helpers.argparser import argparser
from aes_encryption.requirements import *

issue_18249 = "https://github.com/ClickHouse/ClickHouse/issues/18249"
issue_18250 = "https://github.com/ClickHouse/ClickHouse/issues/18250"
issue_18251 = "https://github.com/ClickHouse/ClickHouse/issues/18251"
issue_24029 = "https://github.com/ClickHouse/ClickHouse/issues/24029"

xfails = {
    # encrypt
    "encrypt/invalid key or iv length for mode/mode=\"'aes-???-gcm'\", key_len=??, iv_len=12, aad=True/iv is too short":
     [(Fail, "known issue")],
    "encrypt/invalid key or iv length for mode/mode=\"'aes-???-gcm'\", key_len=??, iv_len=12, aad=True/iv is too long":
     [(Fail, "known issue")],
    "encrypt/invalid plaintext data type/data_type='IPv6', value=\"toIPv6('2001:0db8:0000:85a3:0000:0000:ac1f:8001')\"":
     [(Fail, "known issue as IPv6 is implemented as FixedString(16)")],
    # encrypt_mysql
    "encrypt_mysql/key or iv length for mode/mode=\"'aes-???-ecb'\", key_len=??, iv_len=None":
     [(Fail, issue_18251)],
    "encrypt_mysql/invalid parameters/iv not valid for mode":
     [(Fail, issue_18251)],
    "encrypt_mysql/invalid plaintext data type/data_type='IPv6', value=\"toIPv6('2001:0db8:0000:85a3:0000:0000:ac1f:8001')\"":
     [(Fail, "known issue as IPv6 is implemented as FixedString(16)")],
    # decrypt_mysql
    "decrypt_mysql/key or iv length for mode/mode=\"'aes-???-ecb'\", key_len=??, iv_len=None:":
     [(Fail, issue_18251)],
    # compatibility
    "compatibility/insert/encrypt using materialized view/:":
     [(Fail, issue_18249)],
    "compatibility/insert/decrypt using materialized view/:":
     [(Error, issue_18249)],
    "compatibility/insert/aes encrypt mysql using materialized view/:":
     [(Fail, issue_18249)],
    "compatibility/insert/aes decrypt mysql using materialized view/:":
     [(Error, issue_18249)],
    "compatibility/select/decrypt unique":
     [(Fail, issue_18249)],
    "compatibility/mysql/:engine/decrypt/mysql_datatype='TEXT'/:":
     [(Fail, issue_18250)],
    "compatibility/mysql/:engine/decrypt/mysql_datatype='VARCHAR(100)'/:":
     [(Fail, issue_18250)],
    "compatibility/mysql/:engine/encrypt/mysql_datatype='TEXT'/:":
     [(Fail, issue_18250)],
    "compatibility/mysql/:engine/encrypt/mysql_datatype='VARCHAR(100)'/:":
     [(Fail, issue_18250)],
    # reinterpretAsFixedString for UUID stopped working 
    "decrypt/decryption/mode=:datatype=UUID:":
     [(Fail, issue_24029)], 
    "encrypt/:/mode=:datatype=UUID:":
     [(Fail, issue_24029)], 
    "decrypt/invalid ciphertext/mode=:/invalid ciphertext=reinterpretAsFixedString(toUUID:": 
     [(Fail, issue_24029)], 
    "encrypt_mysql/encryption/mode=:datatype=UUID:":
     [(Fail, issue_24029)], 
    "decrypt_mysql/decryption/mode=:datatype=UUID:":
     [(Fail, issue_24029)], 
    "decrypt_mysql/invalid ciphertext/mode=:/invalid ciphertext=reinterpretAsFixedString(toUUID:":
     [(Fail, issue_24029)], 
}

@TestFeature
@Name("aes encryption")
@ArgumentParser(argparser)
@Specifications(SRS_008_ClickHouse_AES_Encryption_Functions)
@Requirements(
    RQ_SRS008_AES_Functions("1.0"),
    RQ_SRS008_AES_Functions_DifferentModes("1.0")
)
@XFails(xfails)
def regression(self, local, clickhouse_binary_path, stress=None, parallel=None):
    """ClickHouse AES encryption functions regression module.
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
            docker_compose_project_dir=os.path.join(current_dir(), "aes_encryption_env")) as cluster:
        self.context.cluster = cluster

        tasks = []
        with Pool(5) as pool:
            try:
                run_scenario(pool, tasks, Feature(test=load("aes_encryption.tests.encrypt", "feature"), flags=TE))
                run_scenario(pool, tasks, Feature(test=load("aes_encryption.tests.decrypt", "feature"), flags=TE))
                run_scenario(pool, tasks, Feature(test=load("aes_encryption.tests.encrypt_mysql", "feature"), flags=TE))
                run_scenario(pool, tasks, Feature(test=load("aes_encryption.tests.decrypt_mysql", "feature"), flags=TE))
                run_scenario(pool, tasks, Feature(test=load("aes_encryption.tests.compatibility.feature", "feature"), flags=TE))
            finally:
                join(tasks)

if main():
    regression()
