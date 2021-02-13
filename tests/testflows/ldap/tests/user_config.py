import xml.etree.ElementTree as xmltree

from testflows.core import *

from ldap.tests.common import *
from ldap.requirements import *

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_User_Configuration_Invalid("1.0"),
    RQ_SRS_007_LDAP_Configuration_User_Name_Empty("1.0")
)
def empty_user_name(self, timeout=20):
    """Check that empty string as a user name is not allowed.
    """
    servers = {"openldap1": {
        "host": "openldap1", "port": "389", "enable_tls": "no",
        "auth_dn_prefix": "cn=", "auth_dn_suffix": ",ou=users,dc=company,dc=com"
    }}
    users = [{"server": "openldap1", "username": "", "password": "user1", "login": True}]
    config = create_ldap_users_config_content(*users)
    invalid_user_config(servers, config, timeout=timeout)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_User_Configuration_Invalid("1.0"),
    RQ_SRS_007_LDAP_Configuration_User_LDAP_InvalidServerName_Empty("1.0")
)
def empty_server_name(self, timeout=20):
    """Check that if server name is an empty string then login is not allowed.
    """
    servers = {"openldap1": {
        "host": "openldap1", "port": "389", "enable_tls": "no",
        "auth_dn_prefix": "cn=", "auth_dn_suffix": ",ou=users,dc=company,dc=com"
    }}
    users = [{"server": "", "username": "user1", "password": "user1", "login": True,
        "errorcode": 4,
        "message": "DB::Exception: user1: Authentication failed: password is incorrect or there is no user with such name"
    }]
    login(servers, *users)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_User_Configuration_Invalid("1.0"),
    RQ_SRS_007_LDAP_Configuration_User_LDAP_InvalidServerName_NotDefined("1.0")
)
def empty_server_not_defined(self, timeout=20):
    """Check that if server is not defined then login is not allowed.
    """
    servers = {"openldap1": {
        "host": "openldap1", "port": "389", "enable_tls": "no",
        "auth_dn_prefix": "cn=", "auth_dn_suffix": ",ou=users,dc=company,dc=com"
    }}
    users = [{"server": "foo", "username": "user1", "password": "user1", "login": True,
        "errorcode": 36,
        "message": "DB::Exception: LDAP server 'foo' is not configured"
    }]
    login(servers, *users)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Configuration_User_Syntax("1.0")
)
def valid_user_config(self):
    """Check syntax of valid user configuration of LDAP authenticated user."""
    servers = {"openldap1": {
        "host": "openldap1", "port": "389", "enable_tls": "no",
        "auth_dn_prefix": "cn=", "auth_dn_suffix": ",ou=users,dc=company,dc=com"
    }}
    users = [{"server": "openldap1", "username": "user1", "password": "user1", "login": True}]
    login(servers, *users)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Configuration_User_OnlyOneServer("1.0")
)
def multiple_servers(self, timeout=20):
    """Check that user configuration allows to specify only one LDAP server for a given user
    and if multiple servers are specified then the first one is used."""
    servers = {
        "openldap1": {
            "host": "openldap1", "port": "389", "enable_tls": "no",
            "auth_dn_prefix": "cn=", "auth_dn_suffix": ",ou=users,dc=company,dc=com"
        },
        "openldap2": {
            "host": "openldap2", "enable_tls": "yes", "tls_require_cert": "never",
            "auth_dn_prefix": "cn=", "auth_dn_suffix": ",ou=users,dc=company,dc=com"
        },
    }
    user = {"server": "openldap1", "username": "user1", "password": "user1", "login": True}

    with When("I first create regular user configuration file"):
        config = create_ldap_users_config_content(user)

    with And("I modify it to add another server"):
        root = xmltree.fromstring(config.content)
        xml_users = root.find("users")
        xml_users.append(xmltree.Comment(text=f"LDAP users {config.uid}"))
        xml_user_ldap = xml_users.find(user["username"]).find("ldap")
        xml_append(xml_user_ldap, "server", "openldap2")
        xml_indent(root)
        content = xml_with_utf8 + str(xmltree.tostring(root, short_empty_elements=False, encoding="utf-8"), "utf-8")

        new_config = Config(content, config.path, config.name, config.uid, config.preprocessed_name)

    with Then("I login and expect it to work as the first server shall be used"):
        login(servers, user, config=new_config)

@TestScenario
@Requirements(
    RQ_SRS_007_LDAP_Configuration_User_BothPasswordAndLDAP("1.0")
)
def ldap_and_password(self):
    """Check that user can't be authenticated if both `ldap` and `password`
    is specified for the same user. We expect an error message to be present in the log
    and login attempt to fail.
    """
    node = self.context.node
    servers = {
        "openldap1": {
            "host": "openldap1", "port": "389", "enable_tls": "no",
            "auth_dn_prefix": "cn=", "auth_dn_suffix": ",ou=users,dc=company,dc=com"
        },
    }
    user = {
        "server": "openldap1", "username": "user1", "password": "user1", "login": True,
        "errorcode": 4,
        "message": "DB::Exception: user1: Authentication failed: password is incorrect or there is no user with such name"
    }

    with When("I first create regular user configuration file"):
        config = create_ldap_users_config_content(user)

    with And("I modify it to add explicit password"):
        root = xmltree.fromstring(config.content)
        xml_users = root.find("users")
        xml_users.append(xmltree.Comment(text=f"LDAP users {config.uid}"))
        xml_user = xml_users.find(user["username"])
        xml_append(xml_user, "password", "hellothere")
        xml_indent(root)
        content = xml_with_utf8 + str(xmltree.tostring(root, short_empty_elements=False, encoding="utf-8"), "utf-8")

        new_config = Config(content, config.path, config.name, config.uid, config.preprocessed_name)

    error_message = "DB::Exception: More than one field of 'password'"

    with Then("I expect an error when I try to load the configuration file", description=error_message):
        invalid_user_config(servers, new_config, message=error_message, tail=16)

    with And("I expect the authentication to fail when I try to login"):
        login(servers, user, config=new_config)

@TestFeature
@Name("user config")
def feature(self, node="clickhouse1"):
    """Check that server returns an error and prohibits
    user login if LDAP users configuration is not valid.
    """
    self.context.node = self.context.cluster.node(node)

    for scenario in loads(current_module(), Scenario):
        scenario()
