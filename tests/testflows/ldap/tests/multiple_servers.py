from testflows.core import *
from testflows.asserts import error

from ldap.tests.common import login
from ldap.requirements import RQ_SRS_007_LDAP_Authentication_MultipleServers

@TestScenario
@Name("multiple servers")
@Requirements(
    RQ_SRS_007_LDAP_Authentication_MultipleServers("1.0")
)
def scenario(self, node="clickhouse1"):
    """Check that multiple LDAP servers can be used to
    authenticate users.
    """
    self.context.node = self.context.cluster.node(node)
    servers = {
        "openldap1": {
            "host": "openldap1",
            "port": "389",
            "enable_tls": "no",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com"
        },
        "openldap2": {
            "host": "openldap2",
            "port": "636",
            "enable_tls": "yes",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com",
            "tls_require_cert": "never",
        },
    }
    users = [
        {"server": "openldap1", "username": "user1", "password": "user1", "login": True},
        {"server": "openldap2", "username": "user2", "password": "user2", "login": True}
    ]
    login(servers, *users)
