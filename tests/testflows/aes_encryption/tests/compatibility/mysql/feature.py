from testflows.core import *

from aes_encryption.requirements import *


@TestFeature
@Name("mysql")
@Requirements(RQ_SRS008_AES_Functions_Compatibility_MySQL("1.0"))
def feature(self, node="clickhouse1"):
    """Check encryption functions usage compatibility with MySQL."""
    self.context.node = self.context.cluster.node(node)

    Feature(
        run=load("aes_encryption.tests.compatibility.mysql.table_engine", "feature"),
        flags=TE,
    )
    Feature(
        run=load("aes_encryption.tests.compatibility.mysql.database_engine", "feature"),
        flags=TE,
    )
    Feature(
        run=load("aes_encryption.tests.compatibility.mysql.table_function", "feature"),
        flags=TE,
    )
    Feature(
        run=load("aes_encryption.tests.compatibility.mysql.dictionary", "feature"),
        flags=TE,
    )
