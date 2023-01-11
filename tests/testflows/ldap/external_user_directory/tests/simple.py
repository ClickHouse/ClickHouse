from testflows.core import *
from testflows.asserts import error

from ldap.external_user_directory.tests.common import login


@TestScenario
@Name("simple")
def scenario(self, node="clickhouse1"):
    """Check that an LDAP external user directory can be used to authenticate a user."""
    self.context.node = self.context.cluster.node(node)
    servers = {
        "openldap1": {
            "host": "openldap1",
            "port": "389",
            "enable_tls": "no",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com",
        },
    }
    users = [
        {
            "server": "openldap1",
            "username": "user1",
            "password": "user1",
            "login": True,
        },
    ]
    login(servers, "openldap1", *users)
