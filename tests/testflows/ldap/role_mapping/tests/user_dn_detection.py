# -*- coding: utf-8 -*-
import importlib

from testflows.core import *
from testflows.asserts import error

from ldap.role_mapping.requirements import *
from ldap.role_mapping.tests.common import *

@TestOutline
def check_config(self, entries, valid=True, ldap_server="openldap1", user="user1", password="user1"):
    """Apply LDAP server configuration and check login.
    """
    if valid:
        exitcode = 0
        message = "1"
    else:
        exitcode = 4
        message = "DB::Exception: user1: Authentication failed: password is incorrect or there is no user with such name"

    with Given("I add LDAP server configuration"):
        config = create_xml_config_content(entries=entries, config_file="ldap_servers.xml")
        add_ldap_servers_configuration(servers=None, config=config)

    with And("I add LDAP external user directory configuration"):
        add_ldap_external_user_directory(server=ldap_server,
            role_mappings=None, restart=True)

    with When(f"I login I try to login as an LDAP user"):
        r = self.context.node.query(f"SELECT 1", settings=[
            ("user", user), ("password", password)], exitcode=exitcode, message=message)

@TestScenario
@Tags("config")
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_UserDNDetection_BaseDN("1.0")
)
def config_invalid_base_dn(self):
    """Check when invalid `base_dn` is specified in the user_dn_detection section.
    """

    with Given("I define LDAP server configuration with invalid base_dn"):
        entries = {
            "ldap_servers": [
                {
                    "openldap1": {
                        "host": "openldap1",
                        "port": "389",
                        "enable_tls": "no",
                        "bind_dn": "cn={user_name},ou=users,dc=company,dc=com",
                        "user_dn_detection": {
                            "base_dn": "ou=user,dc=company,dc=com",
                            "search_filter": "(&(objectClass=inetOrgPerson)(uid={user_name}))"
                        }
                    }
                }
            ]
        }

    check_config(entries=entries, valid=False)

@TestScenario
@Tags("config")
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_UserDNDetection_BaseDN("1.0")
)
def config_empty_base_dn(self):
    """Check when empty `base_dn` is specified in the user_dn_detection section.
    """
    with Given("I define LDAP server configuration with invalid base_dn"):
        entries = {
            "ldap_servers": [
                {
                    "openldap1": {
                        "host": "openldap1",
                        "port": "389",
                        "enable_tls": "no",
                        "bind_dn": "cn={user_name},ou=users,dc=company,dc=com",
                        "user_dn_detection": {
                            "base_dn": "",
                            "search_filter": "(&(objectClass=inetOrgPerson)(uid={user_name}))"
                        }
                    }
                }
            ]
        }

    check_config(entries=entries, valid=False)

@TestScenario
@Tags("config")
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_UserDNDetection_BaseDN("1.0")
)
def config_missing_base_dn(self):
    """Check when missing `base_dn` is specified in the user_dn_detection section.
    """
    with Given("I define LDAP server configuration with invalid base_dn"):
        entries = {
            "ldap_servers": [
                {
                    "openldap1": {
                        "host": "openldap1",
                        "port": "389",
                        "enable_tls": "no",
                        "bind_dn": "cn={user_name},ou=users,dc=company,dc=com",
                        "user_dn_detection": {
                            "search_filter": "(&(objectClass=inetOrgPerson)(uid={user_name}))"
                        }
                    }
                }
            ]
        }

    check_config(entries=entries, valid=False)

@TestScenario
@Tags("config")
@Requirements(
    # FIXME
)
def config_invalid_search_filter(self):
    """Check when invalid `search_filter` is specified in the user_dn_detection section.
    """
    with Given("I define LDAP server configuration with invalid search_filter"):
        entries = {
            "ldap_servers": [
                {
                    "openldap1": {
                        "host": "openldap1",
                        "port": "389",
                        "enable_tls": "no",
                        "bind_dn": "cn={user_name},ou=users,dc=company,dc=com",
                        "user_dn_detection": {
                            "base_dn": "ou=users,dc=company,dc=com",
                            "search_filter": "(&(objectClass=inetOrgPersons)(uid={user_name}))"
                        }
                    }
                }
            ]
        }

    check_config(entries=entries, valid=False)

@TestScenario
@Tags("config")
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_UserDNDetection_SearchFilter("1.0")
)
def config_missing_search_filter(self):
    """Check when missing `search_filter` is specified in the user_dn_detection section.
    """
    with Given("I define LDAP server configuration with invalid search_filter"):
        entries = {
            "ldap_servers": [
                {
                    "openldap1": {
                        "host": "openldap1",
                        "port": "389",
                        "enable_tls": "no",
                        "bind_dn": "cn={user_name},ou=users,dc=company,dc=com",
                        "user_dn_detection": {
                            "base_dn": "ou=users,dc=company,dc=com",
                        }
                    }
                }
            ]
        }

    check_config(entries=entries, valid=False)

@TestScenario
@Tags("config")
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_UserDNDetection_SearchFilter("1.0")
)
def config_empty_search_filter(self):
    """Check when empty `search_filter` is specified in the user_dn_detection section.
    """
    with Given("I define LDAP server configuration with invalid search_filter"):
        entries = {
            "ldap_servers": [
                {
                    "openldap1": {
                        "host": "openldap1",
                        "port": "389",
                        "enable_tls": "no",
                        "bind_dn": "cn={user_name},ou=users,dc=company,dc=com",
                        "user_dn_detection": {
                            "base_dn": "ou=users,dc=company,dc=com",
                            "search_filter": ""
                        }
                    }
                }
            ]
        }

    check_config(entries=entries, valid=False)

@TestScenario
@Tags("config")
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_UserDNDetection_BaseDN("1.0"),
    RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_UserDNDetection_SearchFilter("1.0")
)
def config_valid(self):
    """Check valid config with valid user_dn_detection section.
    """
    with Given("I define LDAP server configuration"):
        entries = {
            "ldap_servers": [
                {
                    "openldap1": {
                        "host": "openldap1",
                        "port": "389",
                        "enable_tls": "no",
                        "bind_dn": "cn={user_name},ou=users,dc=company,dc=com",
                        "user_dn_detection": {
                            "base_dn": "ou=users,dc=company,dc=com",
                            "search_filter": "(&(objectClass=inetOrgPerson)(uid={user_name}))"
                        }
                    }
                }
            ]
        }

    check_config(entries=entries, valid=True)

@TestScenario
@Tags("config")
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_UserDNDetection_BaseDN("1.0"),
    RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_UserDNDetection_SearchFilter("1.0")
)
def config_valid_tls_connection(self):
    """Check valid config with valid user_dn_detection section when
    using LDAP that is configured to use TLS connection.
    """
    with Given("I define LDAP server configuration"):
        entries = {
            "ldap_servers": [
                {
                    "openldap2": {
                        "host": "openldap2",
                        "port": "636",
                        "enable_tls": "yes",
                        "bind_dn": "cn={user_name},ou=users,dc=company,dc=com",
                        "tls_require_cert": "never",
                        "user_dn_detection": {
                            "base_dn": "ou=users,dc=company,dc=com",
                            "search_filter": "(&(objectClass=inetOrgPerson)(uid={user_name}))"
                        }
                    }
                }
            ]
        }

    check_config(entries=entries, valid=True, ldap_server="openldap2", user="user2", password="user2")

@TestOutline(Scenario)
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_UserDNDetection_Scope("1.0")
)
@Examples("scope base_dn", [
    ("base", "cn=user1,ou=users,dc=company,dc=com"),
    ("one_level","ou=users,dc=company,dc=com"),
    ("children","ou=users,dc=company,dc=com"),
    ("subtree","ou=users,dc=company,dc=com") # default value
])
def check_valid_scope_values(self, scope, base_dn):
    """Check configuration with valid scope values.
    """
    with Given("I define LDAP server configuration"):
        entries = {
            "ldap_servers": [
                {
                    "openldap1": {
                        "host": "openldap1",
                        "port": "389",
                        "enable_tls": "no",
                        "bind_dn": "cn={user_name},ou=users,dc=company,dc=com",
                        "user_dn_detection": {
                            "base_dn": base_dn,
                            "search_filter": "(&(objectClass=inetOrgPerson)(uid={user_name}))",
                            "scope": scope
                        }
                    }
                }
            ]
        }

    check_config(entries=entries, valid=True)

@TestSuite
def mapping(self):
    """Run all role mapping tests with both
    openldap1 and openldap2 configured to use
    user DN detection.
    """
    users = [
        {"server": "openldap1", "username": "user1", "password": "user1", "login": True,
         "dn": "cn=user1,ou=users,dc=company,dc=com"},
    ]

    entries = {
        "ldap_servers": [
            {
                "openldap1": {
                    "host": "openldap1",
                    "port": "389",
                    "enable_tls": "no",
                    "bind_dn": "cn={user_name},ou=users,dc=company,dc=com",
                    "user_dn_detection": {
                        "base_dn": "ou=users,dc=company,dc=com",
                        "search_filter": "(&(objectClass=inetOrgPerson)(uid={user_name}))"
                    }
                },
                "openldap2": {
                    "host": "openldap2",
                    "port": "636",
                    "enable_tls": "yes",
                    "bind_dn": "cn={user_name},ou=users,dc=company,dc=com",
                    "tls_require_cert": "never",
                    "user_dn_detection": {
                        "base_dn": "ou=users,dc=company,dc=com",
                        "search_filter": "(&(objectClass=inetOrgPerson)(uid={user_name}))"
                    }
                }
            },
        ]
    }

    with Given("I add LDAP servers configuration"):
        config = create_xml_config_content(entries=entries, config_file="ldap_servers.xml")
        add_ldap_servers_configuration(servers=None, config=config)

    for scenario in loads(importlib.import_module("ldap.role_mapping.tests.mapping"), Scenario):
        scenario(ldap_server="openldap1", ldap_user=users[0])

@TestOutline
def setup_different_bind_dn_and_user_dn(self, uid, map_by, user_dn_detection):
    """Check that roles get mapped properly when bind_dn and user_dn are different
    by creating LDAP users that have switched uid parameter values.
    """
    with Given("I define LDAP server configuration"):
        entries = {
            "ldap_servers": [
                {
                    "openldap1": {
                        "host": "openldap1",
                        "port": "389",
                        "enable_tls": "no",
                        "bind_dn": "cn={user_name},ou=users,dc=company,dc=com",
                    }
                }
            ]
        }

    if user_dn_detection:
        with And("I enable user dn detection"):
            entries["ldap_servers"][0]["openldap1"]["user_dn_detection"] = {
                "base_dn": "ou=users,dc=company,dc=com",
                "search_filter": "(&(objectClass=inetOrgPerson)(uid={user_name}))",
                "scope": "subtree"
            }

    with And("I define role mappings"):
        role_mappings = [
            {
                "base_dn": "ou=groups,dc=company,dc=com",
                "attribute": "cn",
                "search_filter": f"(&(objectClass=groupOfUniqueNames)(uniquemember={{{map_by}}}))",
                "prefix":""
            }
        ]

    with Given("I add LDAP users"):
        first_user = add_ldap_users(users=[
            {"cn": f"first_user", "userpassword": "user", "uid": "second_user"}
        ])[0]

        second_user = add_ldap_users(users=[
            {"cn": f"second_user", "userpassword": "user", "uid": "first_user"}
        ])[0]

    with Given("I add LDAP groups"):
        groups = add_ldap_groups(groups=({"cn": f"role0_{uid}"}, {"cn": f"role1_{uid}"}))

    with And("I add LDAP user to each LDAP group"):
        with By("adding first group to first user"):
            add_user_to_group_in_ldap(user=first_user, group=groups[0])
        with And("adding second group to second user"):
            add_user_to_group_in_ldap(user=second_user, group=groups[1])

    with And("I add RBAC roles"):
        roles = add_rbac_roles(roles=(f"role0_{uid}", f"role1_{uid}"))

    with Given("I add LDAP server configuration"):
        config = create_xml_config_content(entries=entries, config_file="ldap_servers.xml")
        add_ldap_servers_configuration(servers=None, config=config)

    with And("I add LDAP external user directory configuration"):
        add_ldap_external_user_directory(server=self.context.ldap_node.name,
            role_mappings=role_mappings, restart=True)

@TestScenario
def map_roles_by_user_dn_when_base_dn_and_user_dn_are_different(self):
    """Check the case when we map roles using user_dn then
    the first user has uid of second user and second user
    has uid of first user and configuring user DN detection to
    determine user_dn based on the uid value so that user_dn
    for the first user will be bind_dn of the second user and
    vice versa.
    """
    uid = getuid()

    setup_different_bind_dn_and_user_dn(uid=uid, map_by="user_dn", user_dn_detection=True)

    with When(f"I login as first LDAP user"):
        r = self.context.node.query(f"SHOW GRANTS", settings=[
            ("user", "first_user"), ("password", "user")])

    with Then("I expect the first user to have mapped LDAP roles from second user"):
        assert f"GRANT role1_{uid} TO first_user" in r.output, error()

    with When(f"I login as second LDAP user"):
        r = self.context.node.query(f"SHOW GRANTS", settings=[
            ("user", "second_user"), ("password", "user")])

    with Then("I expect the second user to have mapped LDAP roles from first user"):
        assert f"GRANT role0_{uid} TO second_user" in r.output, error()

@TestScenario
def map_roles_by_bind_dn_when_base_dn_and_user_dn_are_different(self):
    """Check the case when we map roles by bind_dn when bind_dn and user_dn
    are different.
    """
    uid = getuid()

    setup_different_bind_dn_and_user_dn(uid=uid, map_by="bind_dn", user_dn_detection=True)

    with When(f"I login as first LDAP user"):
        r = self.context.node.query(f"SHOW GRANTS", settings=[
            ("user", "first_user"), ("password", "user")])

    with Then("I expect the first user to have no mapped LDAP roles"):
        assert f"GRANT role0_{uid} TO first_user" == r.output, error()

    with When(f"I login as second LDAP user"):
        r = self.context.node.query(f"SHOW GRANTS", settings=[
            ("user", "second_user"), ("password", "user")])

    with Then("I expect the second user to have no mapped LDAP roles"):
        assert f"GRANT role1_{uid} TO second_user" in r.output, error()

@TestFeature
@Name("user dn detection")
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_Configuration_Server_UserDNDetection("1.0")
)
def feature(self):
    """Check LDAP user DN detection.
    """
    self.context.node = self.context.cluster.node("clickhouse1")
    self.context.ldap_node = self.context.cluster.node("openldap1")

    with Given("I fix LDAP access permissions"):
        fix_ldap_permissions(node=self.context.cluster.node("openldap1"))
        fix_ldap_permissions(node=self.context.cluster.node("openldap2"))

    for scenario in ordered(loads(current_module(), Scenario)):
        scenario()

    Suite(run=mapping)
