from testflows.core import *

from aes_encryption.requirements import *

@TestFeature
@Name("compatibility")
@Requirements(
    RQ_SRS008_AES_Functions_DataFromMultipleSources("1.0")
)
def feature(self, node="clickhouse1"):
    """Check encryption functions usage compatibility.
    """
    self.context.node = self.context.cluster.node(node)

    Feature(run=load("aes_encryption.tests.compatibility.insert", "feature"), flags=TE)
    Feature(run=load("aes_encryption.tests.compatibility.select", "feature"), flags=TE)
    Feature(run=load("aes_encryption.tests.compatibility.mysql.feature", "feature"), flags=TE)