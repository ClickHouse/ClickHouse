from testflows.core import *
from testflows.asserts import error

from ldap.role_mapping.requirements import *

from ldap.authentication.tests.common import invalid_server_config
from ldap.external_user_directory.tests.common import login

@TestScenario
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_BindDN("1.0")
)
def valid_bind_dn(self):
    """Check that LDAP users can login when `bind_dn` is valid.
    """
    servers = {
        "openldap1": {
            "host": "openldap1", "port": "389", "enable_tls": "no",
            "bind_dn": "cn={user_name},ou=users,dc=company,dc=com"
        }
    }

    user = {
        "server": "openldap1", "username": "user1", "password": "user1", "login": True,
    }

    login(servers, "openldap1", user)

@TestScenario
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_BindDN("1.0")
)
def invalid_bind_dn(self):
    """Check that LDAP users can't login when `bind_dn` is invalid.
    """
    servers = {
        "openldap1": {
            "host": "openldap1", "port": "389", "enable_tls": "no",
            "bind_dn": "cn={user_name},ou=users,dc=company2,dc=com"
        }}

    user = {
        "server": "openldap1", "username": "user1", "password": "user1", "login": True,
        "exitcode": 4,
        "message": "DB::Exception: user1: Authentication failed: password is incorrect or there is no user with such name."
    }

    login(servers, "openldap1", user)

@TestScenario
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_BindDN_ConflictWith_AuthDN("1.0")
)
def bind_dn_conflict_with_auth_dn(self, timeout=60):
    """Check that an error is returned with both `bind_dn` and
    `auth_dn_prefix` and `auth_dn_suffix` are specified at the same time.
    """
    message = "DB::Exception: Deprecated 'auth_dn_prefix' and 'auth_dn_suffix' entries cannot be used with 'bind_dn' entry"
    servers = {
        "openldap1": {
            "host": "openldap1", "port": "389", "enable_tls": "no",
            "bind_dn": "cn={user_name},ou=users,dc=company,dc=com",
            "auth_dn_prefix": "cn=",
            "auth_dn_suffix": ",ou=users,dc=company,dc=com"
        }
    }

    invalid_server_config(servers, message=message, tail=30, timeout=timeout)


@TestFeature
@Name("server config")
def feature(self, node="clickhouse1"):
    """Check LDAP server configuration.
    """
    self.context.node = self.context.cluster.node(node)
    for scenario in loads(current_module(), Scenario):
        scenario()
