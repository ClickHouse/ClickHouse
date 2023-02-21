# -*- coding: utf-8 -*-
from testflows.core import *
from testflows.asserts import error

from ldap.role_mapping.requirements import *
from ldap.role_mapping.tests.common import *
from ldap.external_user_directory.tests.common import randomword

from ldap.external_user_directory.tests.authentications import (
    login_with_valid_username_and_password,
)
from ldap.external_user_directory.tests.authentications import (
    login_with_invalid_username_and_valid_password,
)
from ldap.external_user_directory.tests.authentications import (
    login_with_valid_username_and_invalid_password,
)


def remove_ldap_groups_in_parallel(groups, i, iterations=10):
    """Remove LDAP groups."""
    with When(f"LDAP groups are removed #{i}"):
        for j in range(iterations):
            for group in groups:
                with When(f"I delete group #{j}", description=f"{group}"):
                    delete_group_from_ldap(group, exitcode=None)


def add_ldap_groups_in_parallel(ldap_user, names, i, iterations=10):
    """Add LDAP groups."""
    with When(f"LDAP groups are added #{i}"):
        for j in range(iterations):
            for name in names:
                with When(f"I add group {name} #{j}", description=f"{name}"):
                    group = add_group_to_ldap(cn=name, exitcode=None)

                    with When(f"I add user to the group"):
                        add_user_to_group_in_ldap(
                            user=ldap_user, group=group, exitcode=None
                        )


def add_user_to_ldap_groups_in_parallel(ldap_user, groups, i, iterations=10):
    """Add user to LDAP groups."""
    with When(f"user is added to LDAP groups #{i}"):
        for j in range(iterations):
            for group in groups:
                with When(f"I add user to the group {group['dn']} #{j}"):
                    add_user_to_group_in_ldap(
                        user=ldap_user, group=group, exitcode=None
                    )


def remove_user_from_ldap_groups_in_parallel(ldap_user, groups, i, iterations=10):
    """Remove user from LDAP groups."""
    with When(f"user is removed from LDAP groups #{i}"):
        for j in range(iterations):
            for group in groups:
                with When(f"I remove user from the group {group['dn']} #{j}"):
                    delete_user_from_group_in_ldap(
                        user=ldap_user, group=group, exitcode=None
                    )


def add_roles_in_parallel(role_names, i, iterations=10):
    """Add roles."""
    with When(f"roles are added #{i}"):
        for j in range(iterations):
            for role_name in role_names:
                with When(f"I add role {role_name} #{j}"):
                    current().context.node.query(f"CREATE ROLE OR REPLACE {role_name}")


def remove_roles_in_parallel(role_names, i, iterations=10):
    """Remove roles."""
    with When(f"roles are removed #{i}"):
        for j in range(iterations):
            for role_name in role_names:
                with When(f"I remove role {role_name} #{j}"):
                    current().context.node.query(f"DROP ROLE IF EXISTS {role_name}")


@TestScenario
@Requirements(RQ_SRS_014_LDAP_RoleMapping_Map_MultipleRoles("1.0"))
def multiple_roles(self, ldap_server, ldap_user):
    """Check that users authenticated using LDAP external user directory
    can be assigned multiple LDAP mapped roles.
    """
    uid = getuid()

    role_mappings = [
        {
            "base_dn": "ou=groups,dc=company,dc=com",
            "attribute": "cn",
            "search_filter": "(&(objectClass=groupOfUniqueNames)(uniquemember={bind_dn}))",
            "prefix": "",
        }
    ]

    with Given("I add LDAP groups"):
        groups = add_ldap_groups(
            groups=({"cn": f"role0_{uid}"}, {"cn": f"role1_{uid}"})
        )

    with And("I add LDAP user to each LDAP group"):
        add_user_to_group_in_ldap(user=ldap_user, group=groups[0])
        add_user_to_group_in_ldap(user=ldap_user, group=groups[1])

    with And("I add RBAC roles"):
        roles = add_rbac_roles(roles=(f"role0_{uid}", f"role1_{uid}"))

    with And("I add LDAP external user directory configuration"):
        add_ldap_external_user_directory(
            server=ldap_server, role_mappings=role_mappings, restart=True
        )

    with When(f"I login as an LDAP user"):
        r = self.context.node.query(
            f"SHOW GRANTS",
            settings=[
                ("user", ldap_user["username"]),
                ("password", ldap_user["password"]),
            ],
        )

    with Then("I expect the user to have mapped LDAP roles"):
        with By(f"checking that first role is assigned", description=f"{roles[0]}"):
            assert roles[0] in r.output, error()
        with And(
            f"checking that second role is also assigned", description=f"{roles[1]}"
        ):
            assert roles[1] in r.output, error()


@TestScenario
@Requirements(RQ_SRS_014_LDAP_RoleMapping_WithFixedRoles("1.0"))
def with_fixed_roles(self, ldap_server, ldap_user):
    """Check that LDAP users can be assigned roles dynamically
    and statically using the `<roles>` section.
    """
    uid = getuid()
    role_name = f"role_{uid}"
    fixed_role_name = f"role_fixed_{uid}"

    role_mappings = [
        {
            "base_dn": "ou=groups,dc=company,dc=com",
            "attribute": "cn",
            "search_filter": "(&(objectClass=groupOfUniqueNames)(uniquemember={bind_dn}))",
            "prefix": "",
        }
    ]

    with Given("I add LDAP group"):
        groups = add_ldap_groups(groups=({"cn": role_name},))

    with And("I add LDAP user to the group"):
        add_user_to_group_in_ldap(user=ldap_user, group=groups[0])

    with And("I add matching RBAC role"):
        mapped_roles = add_rbac_roles(roles=(f"{role_name}",))

    with And("I add an RBAC role that will be added statically"):
        roles = add_rbac_roles(roles=(f"{fixed_role_name}",))

    with And("I add LDAP external user directory configuration"):
        add_ldap_external_user_directory(
            server=ldap_server, role_mappings=role_mappings, roles=roles, restart=True
        )

    with When(f"I login as an LDAP user"):
        r = self.context.node.query(
            f"SHOW GRANTS",
            settings=[
                ("user", ldap_user["username"]),
                ("password", ldap_user["password"]),
            ],
        )

    with Then("I expect the user to have mapped and fixed roles"):
        with By("checking that mapped role is assigned"):
            assert mapped_roles[0].strip("'") in r.output, error()
        with And("checking that fixed role is assigned"):
            assert roles[0] in r.output, error()


@TestOutline
def map_role(
    self, role_name, ldap_server, ldap_user, rbac_role_name=None, role_mappings=None
):
    """Check that we can map a role with a given name."""
    if role_mappings is None:
        role_mappings = [
            {
                "base_dn": "ou=groups,dc=company,dc=com",
                "attribute": "cn",
                "search_filter": "(&(objectClass=groupOfUniqueNames)(uniquemember={bind_dn}))",
                "prefix": "",
            }
        ]

    if rbac_role_name is None:
        rbac_role_name = role_name

    with Given("I add LDAP group"):
        groups = add_ldap_groups(groups=({"cn": role_name},))

    with And("I add LDAP user to the group"):
        add_user_to_group_in_ldap(user=ldap_user, group=groups[0])

    with And("I add matching RBAC role"):
        roles = add_rbac_roles(roles=(f"'{rbac_role_name}'",))

    with And("I add LDAP external user directory configuration"):
        add_ldap_external_user_directory(
            server=ldap_server, role_mappings=role_mappings, restart=True
        )

    with When(f"I login as an LDAP user"):
        r = self.context.node.query(
            f"SHOW GRANTS",
            settings=[
                ("user", ldap_user["username"]),
                ("password", ldap_user["password"]),
            ],
        )

    with Then("I expect the user to have mapped LDAP role"):
        with By(f"checking that the role is assigned", description=f"{role_name}"):
            assert roles[0].strip("'") in r.output, error()


@TestScenario
@Requirements(RQ_SRS_014_LDAP_RoleMapping_Map_Role_Name_WithUTF8Characters("1.0"))
def role_name_with_utf8_characters(self, ldap_server, ldap_user):
    """Check that we can map a role that contains UTF8 characters."""
    uid = getuid()
    role_name = f"role_{uid}_Gãńdåłf_Thê_Gręât"

    map_role(role_name=role_name, ldap_server=ldap_server, ldap_user=ldap_user)


@TestScenario
@Requirements(RQ_SRS_014_LDAP_RoleMapping_Map_Role_Name_Long("1.0"))
def role_name_with_more_than_128_characters(self, ldap_server, ldap_user):
    """Check that we can map a role that contains more than 128 characters."""
    uid = getuid()
    role_name = f"role_{uid}_{'r'*128}"

    map_role(role_name=role_name, ldap_server=ldap_server, ldap_user=ldap_user)


@TestScenario
@Requirements(RQ_SRS_014_LDAP_RoleMapping_Map_Role_Name_WithSpecialXMLCharacters("1.0"))
def role_name_with_special_xml_characters(self, ldap_server, ldap_user):
    """Check that we can map a role that contains special XML
    characters that must be escaped.
    """
    uid = getuid()
    role_name = f"role_{uid}_\\<\\>"
    rbac_role_name = f"role_{uid}_<>"

    map_role(
        role_name=role_name,
        ldap_server=ldap_server,
        ldap_user=ldap_user,
        rbac_role_name=rbac_role_name,
    )


@TestScenario
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_Map_Role_Name_WithSpecialRegexCharacters("1.0")
)
def role_name_with_special_regex_characters(self, ldap_server, ldap_user):
    """Check that we can map a role that contains special regex
    characters that must be escaped.
    """
    uid = getuid()
    role_name = f"role_{uid}_\\+.?$"
    rbac_role_name = f"role_{uid}_+.?$"

    map_role(
        role_name=role_name,
        ldap_server=ldap_server,
        ldap_user=ldap_user,
        rbac_role_name=rbac_role_name,
    )


@TestOutline
def map_groups_with_prefixes(
    self,
    prefixes,
    group_names,
    role_names,
    expected,
    not_expected,
    ldap_server,
    ldap_user,
):
    """Check that we can map multiple groups to roles whith one or more prefixes."""
    role_mappings = []

    for prefix in prefixes:
        role_mappings.append(
            {
                "base_dn": "ou=groups,dc=company,dc=com",
                "attribute": "cn",
                "search_filter": "(&(objectClass=groupOfUniqueNames)(uniquemember={bind_dn}))",
                "prefix": prefix,
            }
        )

    with Given("I add LDAP group"):
        groups = add_ldap_groups(groups=({"cn": name} for name in group_names))

    with And("I add LDAP user to the group"):
        for group in groups:
            add_user_to_group_in_ldap(user=ldap_user, group=group)

    with And("I add RBAC roles"):
        roles = add_rbac_roles(roles=(f"'{name}'" for name in role_names))

    with And("I add LDAP external user directory configuration"):
        add_ldap_external_user_directory(
            server=ldap_server, role_mappings=role_mappings, restart=True
        )

    with When(f"I login as an LDAP user"):
        r = self.context.node.query(
            f"SHOW GRANTS",
            settings=[
                ("user", ldap_user["username"]),
                ("password", ldap_user["password"]),
            ],
        )

    with Then("I expect the user to have mapped roles"):
        with By(
            f"checking that the roles are assigned",
            description=f"{', '.join(expected)}",
        ):
            for name in expected:
                assert name in r.output, error()

    with And("I expect the user not to have mapped roles"):
        with By(
            f"checking that the roles are not assigned",
            description=f"{', '.join(not_expected)}",
        ):
            for name in not_expected:
                assert name not in r.output, error()


@TestScenario
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Syntax("1.0"),
    RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Prefix("1.0"),
)
def prefix_non_empty(self, ldap_server, ldap_user):
    """Check that only group names with specified prefix are mapped to roles
    when prefix is not empty.
    """
    uid = getuid()

    with Given("I define group names"):
        group_names = [f"clickhouse_role_{uid}", f"role0_{uid}"]

    with And("I define role names"):
        role_names = [f"role_{uid}", f"role0_{uid}"]

    with And("I define group prefixes to be mapped"):
        prefixes = ["clickhouse_"]

    with And("I define the expected mapped and not mapped roles"):
        expected = [f"role_{uid}"]
        not_expected = [f"role0_{uid}"]

    map_groups_with_prefixes(
        ldap_server=ldap_server,
        ldap_user=ldap_user,
        prefixes=prefixes,
        group_names=group_names,
        role_names=role_names,
        expected=expected,
        not_expected=not_expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Prefix_Default(
        "1.0"
    )
)
def prefix_default_value(self, ldap_server, ldap_user):
    """Check that when prefix is not specified the default value of prefix
    is empty and therefore ldap groups are mapped directly to roles.
    """
    uid = getuid()
    role_name = f"role_{uid}"

    role_mappings = [
        {
            "base_dn": "ou=groups,dc=company,dc=com",
            "attribute": "cn",
            "search_filter": "(&(objectClass=groupOfUniqueNames)(uniquemember={bind_dn}))",
        }
    ]

    map_role(
        role_name=role_name,
        ldap_server=ldap_server,
        ldap_user=ldap_user,
        role_mappings=role_mappings,
    )


@TestScenario
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Prefix_WithUTF8Characters(
        "1.0"
    )
)
def prefix_with_utf8_characters(self, ldap_server, ldap_user):
    """Check that we can map a role when prefix contains UTF8 characters."""
    uid = getuid()

    with Given("I define group names"):
        group_names = [f"Gãńdåłf_Thê_Gręât_role_{uid}", f"role0_{uid}"]

    with And("I define role names"):
        role_names = [f"role_{uid}", f"role0_{uid}"]

    with And("I define group prefixes to be mapped"):
        prefixes = ["Gãńdåłf_Thê_Gręât_"]

    with And("I define the expected mapped and not mapped roles"):
        expected = [f"role_{uid}"]
        not_expected = [f"role0_{uid}"]

    map_groups_with_prefixes(
        ldap_server=ldap_server,
        ldap_user=ldap_user,
        prefixes=prefixes,
        group_names=group_names,
        role_names=role_names,
        expected=expected,
        not_expected=not_expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_SpecialCharactersEscaping(
        "1.0"
    ),
    RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Prefix_WithSpecialXMLCharacters(
        "1.0"
    ),
)
def prefix_with_special_xml_characters(self, ldap_server, ldap_user):
    """Check that we can map a role when prefix contains special XML characters."""
    uid = getuid()

    with Given("I define group names"):
        group_names = [f"clickhouse\\<\\>_role_{uid}", f"role0_{uid}"]

    with And("I define role names"):
        role_names = [f"role_{uid}", f"role0_{uid}"]

    with And("I define group prefixes to be mapped"):
        prefixes = ["clickhouse<>_"]

    with And("I define the expected mapped and not mapped roles"):
        expected = [f"role_{uid}"]
        not_expected = [f"role0_{uid}"]

    map_groups_with_prefixes(
        ldap_server=ldap_server,
        ldap_user=ldap_user,
        prefixes=prefixes,
        group_names=group_names,
        role_names=role_names,
        expected=expected,
        not_expected=not_expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_Prefix_WithSpecialRegexCharacters(
        "1.0"
    )
)
def prefix_with_special_regex_characters(self, ldap_server, ldap_user):
    """Check that we can map a role when prefix contains special regex characters."""
    uid = getuid()

    with Given("I define group names"):
        group_names = [f"clickhouse\\+.?\\$_role_{uid}", f"role0_{uid}"]

    with And("I define role names"):
        role_names = [f"role_{uid}", f"role0_{uid}"]

    with And("I define group prefixes to be mapped"):
        prefixes = ["clickhouse+.?\\$_"]

    with And("I define the expected mapped and not mapped roles"):
        expected = [f"role_{uid}"]
        not_expected = [f"role0_{uid}"]

    map_groups_with_prefixes(
        ldap_server=ldap_server,
        ldap_user=ldap_user,
        prefixes=prefixes,
        group_names=group_names,
        role_names=role_names,
        expected=expected,
        not_expected=not_expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_MultipleSections(
        "1.0"
    )
)
def multiple_sections_with_different_prefixes(self, ldap_server, ldap_user):
    """Check that we can map multiple roles with multiple role mapping sections
    that use different prefixes.
    """
    uid = getuid()

    with Given("I define group names"):
        group_names = [
            f"clickhouse0_role0_{uid}",
            f"clickhouse1_role1_{uid}",
            f"role2_{uid}",
        ]

    with And("I define role names"):
        role_names = [f"role0_{uid}", f"role1_{uid}", f"role2_{uid}"]

    with And("I define group prefixes to be mapped"):
        prefixes = ["clickhouse0_", "clickhouse1_"]

    with And("I define the expected mapped and not mapped roles"):
        expected = [f"role0_{uid}", f"role1_{uid}"]
        not_expected = [f"role2_{uid}"]

    map_groups_with_prefixes(
        ldap_server=ldap_server,
        ldap_user=ldap_user,
        prefixes=prefixes,
        group_names=group_names,
        role_names=role_names,
        expected=expected,
        not_expected=not_expected,
    )


@TestScenario
@Requirements(RQ_SRS_014_LDAP_RoleMapping_LDAP_Group_Removed("1.0"))
def group_removed(self, ldap_server, ldap_user):
    """Check that roles are not mapped after the corresponding LDAP group
    is removed.
    """
    uid = getuid()
    role_name = f"role_{uid}"

    role_mappings = [
        {
            "base_dn": "ou=groups,dc=company,dc=com",
            "attribute": "cn",
            "search_filter": "(&(objectClass=groupOfUniqueNames)(uniquemember={bind_dn}))",
            "prefix": "",
        }
    ]

    try:
        with Given("I add LDAP group"):
            group = add_group_to_ldap(**{"cn": role_name})

        with And("I add LDAP user to the group"):
            add_user_to_group_in_ldap(user=ldap_user, group=group)

        with And("I add matching RBAC role"):
            roles = add_rbac_roles(roles=(f"{role_name}",))

        with And("I add LDAP external user directory configuration"):
            add_ldap_external_user_directory(
                server=ldap_server, role_mappings=role_mappings, restart=True
            )

        with When(f"I login as an LDAP user"):
            r = self.context.node.query(
                f"SHOW GRANTS",
                settings=[
                    ("user", ldap_user["username"]),
                    ("password", ldap_user["password"]),
                ],
            )

        with Then("I expect the user to have mapped LDAP role"):
            with By(f"checking that the role is assigned", description=f"{role_name}"):
                assert role_name in r.output, error()
    finally:
        with Finally("I remove LDAP group"):
            delete_group_from_ldap(group)

    with When(f"I login as an LDAP user after LDAP group is removed"):
        r = self.context.node.query(
            f"SHOW GRANTS",
            settings=[
                ("user", ldap_user["username"]),
                ("password", ldap_user["password"]),
            ],
        )

    with Then("I expect the user not to have mapped LDAP role"):
        with By(f"checking that the role is not assigned", description=f"{role_name}"):
            assert role_name not in r.output, error()


@TestScenario
@Requirements(RQ_SRS_014_LDAP_RoleMapping_LDAP_Group_UserRemoved("1.0"))
def user_removed_from_group(self, ldap_server, ldap_user):
    """Check that roles are not mapped after the user has been removed
    from the corresponding LDAP group.
    """
    uid = getuid()
    role_name = f"role_{uid}"

    role_mappings = [
        {
            "base_dn": "ou=groups,dc=company,dc=com",
            "attribute": "cn",
            "search_filter": "(&(objectClass=groupOfUniqueNames)(uniquemember={bind_dn}))",
            "prefix": "",
        }
    ]

    with Given("I add LDAP group"):
        groups = add_ldap_groups(groups=({"cn": role_name},))

    with And("I add LDAP user to the group"):
        add_user_to_group_in_ldap(user=ldap_user, group=groups[0])

    with And("I add matching RBAC role"):
        roles = add_rbac_roles(roles=(f"{role_name}",))

    with And("I add LDAP external user directory configuration"):
        add_ldap_external_user_directory(
            server=ldap_server, role_mappings=role_mappings, restart=True
        )

    with When(f"I login as an LDAP user"):
        r = self.context.node.query(
            f"SHOW GRANTS",
            settings=[
                ("user", ldap_user["username"]),
                ("password", ldap_user["password"]),
            ],
        )

    with Then("I expect the user to have mapped LDAP role"):
        with By(f"checking that the role is assigned", description=f"{role_name}"):
            assert role_name in r.output, error()

    with When("I remove user from the LDAP group"):
        delete_user_from_group_in_ldap(user=ldap_user, group=groups[0])

    with And(f"I login as an LDAP user after user has been removed from the group"):
        r = self.context.node.query(
            f"SHOW GRANTS",
            settings=[
                ("user", ldap_user["username"]),
                ("password", ldap_user["password"]),
            ],
        )

    with Then("I expect the user not to have mapped LDAP role"):
        with By(f"checking that the role is not assigned", description=f"{role_name}"):
            assert role_name not in r.output, error()


@TestScenario
@Requirements(RQ_SRS_014_LDAP_RoleMapping_RBAC_Role_NotPresent("1.0"))
def role_not_present(self, ldap_server, ldap_user):
    """Check that LDAP users can still be authenticated even if
    the mapped role is not present.
    """
    uid = getuid()
    role_name = f"role_{uid}"

    role_mappings = [
        {
            "base_dn": "ou=groups,dc=company,dc=com",
            "attribute": "cn",
            "search_filter": "(&(objectClass=groupOfUniqueNames)(uniquemember={bind_dn}))",
            "prefix": "",
        }
    ]

    with Given("I add LDAP group"):
        groups = add_ldap_groups(groups=({"cn": role_name},))

    with And("I add LDAP user to the group for which no matching roles are present"):
        add_user_to_group_in_ldap(user=ldap_user, group=groups[0])

    with And("I add LDAP external user directory configuration"):
        add_ldap_external_user_directory(
            server=ldap_server, role_mappings=role_mappings, restart=True
        )

    with When(f"I login as an LDAP user"):
        r = self.context.node.query(
            f"SHOW GRANTS",
            settings=[
                ("user", ldap_user["username"]),
                ("password", ldap_user["password"]),
            ],
            no_checks=True,
        )

    with Then("I expect the login to succeed"):
        assert r.exitcode == 0, error()

    with And("the user not to have any mapped LDAP role"):
        assert r.output == "", error()


@TestScenario
@Requirements(RQ_SRS_014_LDAP_RoleMapping_RBAC_Role_NotPresent("1.0"))
def add_new_role_not_present(self, ldap_server, ldap_user):
    """Check that LDAP user can still authenticate when the LDAP
    user is added to a new LDAP group that does not match any existing
    RBAC roles while having other role being already mapped.
    """
    uid = getuid()
    role_name = f"role_{uid}"

    role_mappings = [
        {
            "base_dn": "ou=groups,dc=company,dc=com",
            "attribute": "cn",
            "search_filter": "(&(objectClass=groupOfUniqueNames)(uniquemember={bind_dn}))",
            "prefix": "clickhouse_",
        }
    ]

    with Given("I add LDAP group"):
        groups = add_ldap_groups(groups=({"cn": "clickhouse_" + role_name},))

    with And("I add LDAP user to the group"):
        add_user_to_group_in_ldap(user=ldap_user, group=groups[0])

    with And("I add matching RBAC role"):
        roles = add_rbac_roles(roles=(f"{role_name}",))

    with And("I add LDAP external user directory configuration"):
        add_ldap_external_user_directory(
            server=ldap_server, role_mappings=role_mappings, restart=True
        )

    with When(f"I login as an LDAP user"):
        r = self.context.node.query(
            f"SHOW GRANTS",
            settings=[
                ("user", ldap_user["username"]),
                ("password", ldap_user["password"]),
            ],
            no_checks=True,
        )

        with Then("I expect the login to succeed"):
            assert r.exitcode == 0, error()

        with And("the user should have the mapped LDAP role"):
            assert f"{role_name}" in r.output, error()

    with When("I add LDAP group that maps to unknown role"):
        unknown_groups = add_ldap_groups(
            groups=({"cn": "clickhouse_" + role_name + "_unknown"},)
        )

    with And("I add LDAP user to the group that maps to unknown role"):
        add_user_to_group_in_ldap(user=ldap_user, group=unknown_groups[0])

    with And(f"I again login as an LDAP user"):
        r = self.context.node.query(
            f"SHOW GRANTS",
            settings=[
                ("user", ldap_user["username"]),
                ("password", ldap_user["password"]),
            ],
            no_checks=True,
        )

        with Then("I expect the login to succeed"):
            assert r.exitcode == 0, error()

        with And("the user should still have the present mapped LDAP role"):
            assert f"{role_name}" in r.output, error()

    with When("I add matching previously unknown RBAC role"):
        unknown_roles = add_rbac_roles(roles=(f"{role_name}_unknown",))

    with And(
        f"I again login as an LDAP user after previously unknown RBAC role has been added"
    ):
        r = self.context.node.query(
            f"SHOW GRANTS",
            settings=[
                ("user", ldap_user["username"]),
                ("password", ldap_user["password"]),
            ],
            no_checks=True,
        )

        with Then("I expect the login to succeed"):
            assert r.exitcode == 0, error()

        with And("the user should still have the first mapped LDAP role"):
            assert f"{role_name}" in r.output, error()

        with And("the user should have the previously unknown mapped LDAP role"):
            assert f"{role_name}_unknown" in r.output, error()


@TestScenario
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_RBAC_Role_Removed("1.0"),
    RQ_SRS_014_LDAP_RoleMapping_RBAC_Role_Readded("1.0"),
)
def role_removed_and_readded(self, ldap_server, ldap_user):
    """Check that when a mapped role is removed the privileges provided by the role
    are revoked from all the authenticated LDAP users and when the role
    is added back the privileges to the authenticated LDAP users are re-granted.
    """
    uid = getuid()
    role_name = f"role_{uid}"

    role_mappings = [
        {
            "base_dn": "ou=groups,dc=company,dc=com",
            "attribute": "cn",
            "search_filter": "(&(objectClass=groupOfUniqueNames)(uniquemember={bind_dn}))",
            "prefix": "",
        }
    ]
    with Given("I add LDAP group"):
        groups = add_ldap_groups(groups=({"cn": role_name},))

    with And("I add LDAP user to the group"):
        add_user_to_group_in_ldap(user=ldap_user, group=groups[0])

    with And("I add matching RBAC role"):
        roles = add_rbac_roles(roles=(f"{role_name}",))

    with And("I create a table for which the role will provide privilege"):
        table_name = create_table(
            name=f"table_{uid}",
            create_statement="CREATE TABLE {name} (d DATE, s String, i UInt8) ENGINE = Memory()",
        )

    with And("I grant select privilege on the table to the role"):
        self.context.node.query(f"GRANT SELECT ON {table_name} TO {role_name}")

    with And("I add LDAP external user directory configuration"):
        add_ldap_external_user_directory(
            server=ldap_server, role_mappings=role_mappings, restart=True
        )

    with When(f"I login as LDAP user using clickhouse-client"):
        with self.context.cluster.shell(node=self.context.node.name) as shell:
            with shell(
                f"TERM=dumb clickhouse client --user {ldap_user['username']} --password {ldap_user['password']}",
                asynchronous=True,
                name="client",
            ) as client:
                client.app.expect("clickhouse1 :\) ")

                with When("I execute SHOW GRANTS"):
                    client.app.send(f"SHOW GRANTS")

                    with Then("I expect the user to have the mapped role"):
                        client.app.expect(f"{role_name}")
                        client.app.expect("clickhouse1 :\) ")

                with When("I execute select on the table"):
                    client.app.send(f"SELECT * FROM {table_name} LIMIT 1")

                    with Then("I expect to get no errors"):
                        client.app.expect("Ok\.")
                        client.app.expect("clickhouse1 :\) ")

                with When("I remove the role that grants the privilege"):
                    self.context.node.query(f"DROP ROLE {role_name}")

                with And("I re-execute select on the table"):
                    client.app.send(f"SELECT * FROM {table_name} LIMIT 1")

                    with Then("I expect to get not enough privileges error"):
                        client.app.expect(
                            f"DB::Exception: {ldap_user['username']}: Not enough privileges."
                        )
                        client.app.expect("clickhouse1 :\) ")

                with When("I add the role that grant the privilege back"):
                    self.context.node.query(f"CREATE ROLE {role_name}")
                    self.context.node.query(
                        f"GRANT SELECT ON {table_name} TO {role_name}"
                    )

                with And("I execute select on the table after role is added back"):
                    client.app.send(f"SELECT * FROM {table_name} LIMIT 1")

                    with Then("I expect to get no errors"):
                        client.app.expect("Ok\.")
                        client.app.expect("clickhouse1 :\) ")


@TestScenario
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_RBAC_Role_NewPrivilege("1.0"),
    RQ_SRS_014_LDAP_RoleMapping_RBAC_Role_RemovedPrivilege("1.0"),
)
def privilege_new_and_removed(self, ldap_server, ldap_user):
    """Check that when a new privilege is added to the mapped role
    it is granted to all authenticated LDAP users and when
    the privilege is removed from the role it is also revoked
    from all authenticated LDAP users.
    """
    uid = getuid()
    role_name = f"role_{uid}"

    role_mappings = [
        {
            "base_dn": "ou=groups,dc=company,dc=com",
            "attribute": "cn",
            "search_filter": "(&(objectClass=groupOfUniqueNames)(uniquemember={bind_dn}))",
            "prefix": "",
        }
    ]
    with Given("I add LDAP group"):
        groups = add_ldap_groups(groups=({"cn": role_name},))

    with And("I add LDAP user to the group"):
        add_user_to_group_in_ldap(user=ldap_user, group=groups[0])

    with And("I add matching RBAC role"):
        roles = add_rbac_roles(roles=(f"{role_name}",))

    with And("I create a table for which the role will provide privilege"):
        table_name = create_table(
            name=f"table_{uid}",
            create_statement="CREATE TABLE {name} (d DATE, s String, i UInt8) ENGINE = Memory()",
        )

    with And("I add LDAP external user directory configuration"):
        add_ldap_external_user_directory(
            server=ldap_server, role_mappings=role_mappings, restart=True
        )

    with When(f"I login as LDAP user using clickhouse-client"):
        with self.context.cluster.shell(node=self.context.node.name) as shell:
            with shell(
                f"TERM=dumb clickhouse client --user {ldap_user['username']} --password {ldap_user['password']}",
                asynchronous=True,
                name="client",
            ) as client:
                client.app.expect("clickhouse1 :\) ")

                with When("I execute SHOW GRANTS"):
                    client.app.send(f"SHOW GRANTS")

                    with Then("I expect the user to have the mapped role"):
                        client.app.expect(f"{role_name}")
                        client.app.expect("clickhouse1 :\) ")

                with And(
                    "I execute select on the table when the mapped role does not provide this privilege"
                ):
                    client.app.send(f"SELECT * FROM {table_name} LIMIT 1")

                    with Then("I expect to get not enough privileges error"):
                        client.app.expect(
                            f"DB::Exception: {ldap_user['username']}: Not enough privileges."
                        )
                        client.app.expect("clickhouse1 :\) ")

                with When("I grant select privilege on the table to the mapped role"):
                    self.context.node.query(
                        f"GRANT SELECT ON {table_name} TO {role_name}"
                    )

                with And("I execute select on the table"):
                    client.app.send(f"SELECT * FROM {table_name} LIMIT 1")

                    with Then("I expect to get no errors"):
                        client.app.expect("Ok\.")
                        client.app.expect("clickhouse1 :\) ")

                with When("I remove the privilege from the mapped role"):
                    self.context.node.query(
                        f"REVOKE SELECT ON {table_name} FROM {role_name}"
                    )

                with And("I re-execute select on the table"):
                    client.app.send(f"SELECT * FROM {table_name} LIMIT 1")

                    with Then("I expect to get not enough privileges error"):
                        client.app.expect(
                            f"DB::Exception: {ldap_user['username']}: Not enough privileges."
                        )
                        client.app.expect("clickhouse1 :\) ")


@TestScenario
@Requirements(RQ_SRS_014_LDAP_RoleMapping_RBAC_Role_Added("1.0"))
def role_added(self, ldap_server, ldap_user):
    """Check that when the mapped role is not present during LDAP user authentication but
    is later added then the authenticated LDAP users is granted the privileges provided
    by the mapped role.
    """
    uid = getuid()
    role_name = f"role_{uid}"

    role_mappings = [
        {
            "base_dn": "ou=groups,dc=company,dc=com",
            "attribute": "cn",
            "search_filter": "(&(objectClass=groupOfUniqueNames)(uniquemember={bind_dn}))",
            "prefix": "",
        }
    ]
    with Given("I add LDAP group"):
        groups = add_ldap_groups(groups=({"cn": role_name},))

    with And("I add LDAP user to the group"):
        add_user_to_group_in_ldap(user=ldap_user, group=groups[0])

    with And("I create a table for which the role will provide privilege"):
        table_name = create_table(
            name=f"table_{uid}",
            create_statement="CREATE TABLE {name} (d DATE, s String, i UInt8) ENGINE = Memory()",
        )

    with And("I add LDAP external user directory configuration"):
        add_ldap_external_user_directory(
            server=ldap_server, role_mappings=role_mappings, restart=True
        )

    with When(f"I login as LDAP user using clickhouse-client"):
        with self.context.cluster.shell(node=self.context.node.name) as shell:
            with shell(
                f"TERM=dumb clickhouse client --user {ldap_user['username']} --password {ldap_user['password']}",
                asynchronous=True,
                name="client",
            ) as client:
                client.app.expect("clickhouse1 :\) ")

                with When("I execute SHOW GRANTS"):
                    client.app.send(f"SHOW GRANTS")

                    with Then("I expect the user not to have any mapped role"):
                        client.app.expect(f"Ok\.")
                        client.app.expect("clickhouse1 :\) ")

                with And("I execute select on the table"):
                    client.app.send(f"SELECT * FROM {table_name} LIMIT 1")

                    with Then("I expect to get not enough privileges error"):
                        client.app.expect(
                            f"DB::Exception: {ldap_user['username']}: Not enough privileges."
                        )
                        client.app.expect("clickhouse1 :\) ")

                with When("I add the role that grant the privilege"):
                    self.context.node.query(f"CREATE ROLE {role_name}")
                    self.context.node.query(
                        f"GRANT SELECT ON {table_name} TO {role_name}"
                    )

                with And("I execute select on the table after role is added"):
                    client.app.send(f"SELECT * FROM {table_name} LIMIT 1")

                    with Then("I expect to get no errors"):
                        client.app.expect("Ok\.")
                        client.app.expect("clickhouse1 :\) ")


@TestScenario
@Requirements(RQ_SRS_014_LDAP_RoleMapping_RBAC_Role_New("1.0"))
def role_new(self, ldap_server, ldap_user):
    """Check that no new roles can be granted to LDAP authenticated users."""
    uid = getuid()
    role_name = f"role_{uid}"

    role_mappings = [
        {
            "base_dn": "ou=groups,dc=company,dc=com",
            "attribute": "cn",
            "search_filter": "(&(objectClass=groupOfUniqueNames)(uniquemember={bind_dn}))",
            "prefix": "",
        }
    ]

    message = f"DB::Exception: Cannot update user `{ldap_user['username']}` in ldap because this storage is readonly"
    exitcode = 239

    with Given("I a have RBAC role that is not mapped"):
        roles = add_rbac_roles(roles=(f"{role_name}",))

    with And("I add LDAP external user directory configuration"):
        add_ldap_external_user_directory(
            server=ldap_server, role_mappings=role_mappings, restart=True
        )

    with When(f"I login as LDAP user using clickhouse-client"):
        with self.context.cluster.shell(node=self.context.node.name) as shell:
            with shell(
                f"TERM=dumb clickhouse client --user {ldap_user['username']} --password {ldap_user['password']}",
                asynchronous=True,
                name="client",
            ) as client:
                client.app.expect("clickhouse1 :\) ")

                with When("I try to grant new role to user"):
                    self.context.node.query(
                        f"GRANT {role_name} TO {ldap_user['username']}",
                        message=message,
                        exitcode=exitcode,
                    )


@TestScenario
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_Configuration_UserDirectory_RoleMapping_MultipleSections_IdenticalParameters(
        "1.0"
    )
)
def multiple_sections_with_identical_parameters(self, ldap_server, ldap_user):
    """Check behaviour when multiple role mapping sections
    have exactly the same parameters.
    """
    uid = getuid()
    role_name = f"role_{uid}"

    role_mappings = [
        {
            "base_dn": "ou=groups,dc=company,dc=com",
            "attribute": "cn",
            "search_filter": "(&(objectClass=groupOfUniqueNames)(uniquemember={bind_dn}))",
            "prefix": "",
        }
    ] * 4

    with Given("I add LDAP group"):
        groups = add_ldap_groups(groups=({"cn": role_name},))

    with And("I add LDAP user to the group"):
        add_user_to_group_in_ldap(user=ldap_user, group=groups[0])

    with And("I add matching RBAC role"):
        roles = add_rbac_roles(roles=(f"{role_name}",))

    with And("I add LDAP external user directory configuration"):
        add_ldap_external_user_directory(
            server=ldap_server, role_mappings=role_mappings, restart=True
        )

    with When(f"I login as an LDAP user"):
        r = self.context.node.query(
            f"SHOW GRANTS",
            settings=[
                ("user", ldap_user["username"]),
                ("password", ldap_user["password"]),
            ],
        )

    with Then("I expect the user to have mapped LDAP role"):
        with By(f"checking that the role is assigned", description=f"{role_name}"):
            assert roles[0].strip("'") in r.output, error()


@TestScenario
@Requirements(RQ_SRS_014_LDAP_RoleMapping_LDAP_Group_RemovedAndAdded_Parallel("1.0"))
def group_removed_and_added_in_parallel(
    self, ldap_server, ldap_user, count=20, timeout=200
):
    """Check that user can be authenticated successfully when LDAP groups
    are removed and added in parallel.
    """
    uid = getuid()
    role_names = [f"role{i}_{uid}" for i in range(count)]
    users = [{"cn": ldap_user["username"], "userpassword": ldap_user["password"]}]
    groups = []

    role_mappings = [
        {
            "base_dn": "ou=groups,dc=company,dc=com",
            "attribute": "cn",
            "search_filter": "(&(objectClass=groupOfUniqueNames)(uniquemember={bind_dn}))",
            "prefix": "",
        }
    ]

    try:
        with Given("I initially add all LDAP groups"):
            for role_name in role_names:
                with When(f"I add LDAP groop {role_name}"):
                    group = add_group_to_ldap(**{"cn": role_name})
                with And(f"I add LDAP user to the group {role_name}"):
                    add_user_to_group_in_ldap(user=ldap_user, group=group)
                groups.append(group)

        with And("I add RBAC roles"):
            add_rbac_roles(roles=role_names)

        with And("I add LDAP external user directory configuration"):
            add_ldap_external_user_directory(
                server=ldap_server, role_mappings=role_mappings, restart=True
            )

        tasks = []
        with Pool(4) as pool:
            try:
                with When(
                    "user try to login while LDAP groups are added and removed in parallel"
                ):
                    for i in range(10):
                        tasks.append(
                            pool.submit(
                                login_with_valid_username_and_password,
                                (
                                    users,
                                    i,
                                    50,
                                ),
                            )
                        )
                        tasks.append(
                            pool.submit(
                                remove_ldap_groups_in_parallel,
                                (
                                    groups,
                                    i,
                                    10,
                                ),
                            )
                        )
                        tasks.append(
                            pool.submit(
                                add_ldap_groups_in_parallel,
                                (
                                    ldap_user,
                                    role_names,
                                    i,
                                    10,
                                ),
                            )
                        )
            finally:
                with Finally("it should work", flags=TE):
                    for task in tasks:
                        task.result(timeout=timeout)
    finally:
        with Finally("I clean up all LDAP groups"):
            for group in groups:
                delete_group_from_ldap(group, exitcode=None)


@TestScenario
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_LDAP_Group_UserRemovedAndAdded_Parallel("1.0")
)
def user_removed_and_added_in_ldap_groups_in_parallel(
    self, ldap_server, ldap_user, count=20, timeout=200
):
    """Check that user can be authenticated successfully when it is
    removed and added from mapping LDAP groups in parallel.
    """
    uid = getuid()
    role_names = [f"role{i}_{uid}" for i in range(count)]
    users = [{"cn": ldap_user["username"], "userpassword": ldap_user["password"]}]
    groups = [{"cn": role_name} for role_name in role_names]

    role_mappings = [
        {
            "base_dn": "ou=groups,dc=company,dc=com",
            "attribute": "cn",
            "search_filter": "(&(objectClass=groupOfUniqueNames)(uniquemember={bind_dn}))",
            "prefix": "",
        }
    ]

    with Given("I add all LDAP groups"):
        groups = add_ldap_groups(groups=groups)

        for group in groups:
            with And(f"I add LDAP user to the group {group['dn']}"):
                add_user_to_group_in_ldap(user=ldap_user, group=group)

    with And("I add RBAC roles"):
        add_rbac_roles(roles=role_names)

    with And("I add LDAP external user directory configuration"):
        add_ldap_external_user_directory(
            server=ldap_server, role_mappings=role_mappings, restart=True
        )

    tasks = []
    with Pool(4) as pool:
        try:
            with When(
                "user try to login while user is added and removed from LDAP groups in parallel"
            ):
                for i in range(10):
                    tasks.append(
                        pool.submit(
                            login_with_valid_username_and_password,
                            (
                                users,
                                i,
                                50,
                            ),
                        )
                    )
                    tasks.append(
                        pool.submit(
                            remove_user_from_ldap_groups_in_parallel,
                            (
                                ldap_user,
                                groups,
                                i,
                                1,
                            ),
                        )
                    )
                    tasks.append(
                        pool.submit(
                            add_user_to_ldap_groups_in_parallel,
                            (
                                ldap_user,
                                groups,
                                i,
                                1,
                            ),
                        )
                    )
        finally:
            with Finally("it should work", flags=TE):
                for task in tasks:
                    task.result(timeout=timeout)


@TestScenario
@Requirements(RQ_SRS_014_LDAP_RoleMapping_RBAC_Role_RemovedAndAdded_Parallel("1.0"))
def roles_removed_and_added_in_parallel(
    self, ldap_server, ldap_user, count=20, timeout=200
):
    """Check that user can be authenticated successfully when roles that are mapped
    by the LDAP groups are removed and added in parallel.
    """
    uid = getuid()
    role_names = [f"role{i}_{uid}" for i in range(count)]
    users = [{"cn": ldap_user["username"], "userpassword": ldap_user["password"]}]
    groups = [{"cn": role_name} for role_name in role_names]

    role_mappings = [
        {
            "base_dn": "ou=groups,dc=company,dc=com",
            "attribute": "cn",
            "search_filter": "(&(objectClass=groupOfUniqueNames)(uniquemember={bind_dn}))",
            "prefix": "",
        }
    ]

    fail("known bug that needs to be investigated")

    with Given("I add all LDAP groups"):
        groups = add_ldap_groups(groups=groups)
        for group in groups:
            with And(f"I add LDAP user to the group {group['dn']}"):
                add_user_to_group_in_ldap(user=ldap_user, group=group)

    with And("I add RBAC roles"):
        add_rbac_roles(roles=role_names)

    with And("I add LDAP external user directory configuration"):
        add_ldap_external_user_directory(
            server=ldap_server, role_mappings=role_mappings, restart=True
        )

    tasks = []
    with Pool(4) as pool:
        try:
            with When(
                "user try to login while mapped roles are added and removed in parallel"
            ):
                for i in range(10):
                    tasks.append(
                        pool.submit(
                            login_with_valid_username_and_password,
                            (
                                users,
                                i,
                                50,
                            ),
                        )
                    )
                    tasks.append(
                        pool.submit(
                            remove_roles_in_parallel,
                            (
                                role_names,
                                i,
                                10,
                            ),
                        )
                    )
                    tasks.append(
                        pool.submit(
                            add_roles_in_parallel,
                            (
                                role_names,
                                i,
                                10,
                            ),
                        )
                    )
        finally:
            with Finally("it should work", flags=TE):
                for task in tasks:
                    task.result(timeout=timeout)

            with And("I clean up all the roles"):
                for role_name in role_names:
                    with By(f"dropping role {role_name}", flags=TE):
                        self.context.node.query(f"DROP ROLE IF EXISTS {role_name}")


@TestOutline
def parallel_login(
    self, ldap_server, ldap_user, user_count=10, timeout=200, role_count=10
):
    """Check that login of valid and invalid LDAP authenticated users
    with mapped roles works in parallel.
    """
    uid = getuid()

    role_names = [f"role{i}_{uid}" for i in range(role_count)]
    users = [
        {"cn": f"parallel_user{i}", "userpassword": randomword(20)}
        for i in range(user_count)
    ]
    groups = [{"cn": f"clickhouse_{role_name}"} for role_name in role_names]

    role_mappings = [
        {
            "base_dn": "ou=groups,dc=company,dc=com",
            "attribute": "cn",
            "search_filter": "(&(objectClass=groupOfUniqueNames)(uniquemember={bind_dn}))",
            "prefix": "clickhouse_",
        }
    ]

    with Given("I add LDAP users"):
        users = add_ldap_users(users=users)

    with And("I add all LDAP groups"):
        groups = add_ldap_groups(groups=groups)

    for group in groups:
        for user in users:
            with And(f"I add LDAP user {user['dn']} to the group {group['dn']}"):
                add_user_to_group_in_ldap(user=user, group=group)

    with And("I add RBAC roles"):
        add_rbac_roles(roles=role_names)

    with And("I add LDAP external user directory configuration"):
        add_ldap_external_user_directory(
            server=ldap_server, role_mappings=role_mappings, restart=True
        )

    tasks = []
    with Pool(4) as pool:
        try:
            with When(
                "users try to login in parallel",
                description="""
                * with valid username and password
                * with invalid username and valid password
                * with valid username and invalid password
                """,
            ):
                for i in range(10):
                    tasks.append(
                        pool.submit(
                            login_with_valid_username_and_password,
                            (
                                users,
                                i,
                                50,
                            ),
                        )
                    )
                    tasks.append(
                        pool.submit(
                            login_with_valid_username_and_invalid_password,
                            (
                                users,
                                i,
                                50,
                            ),
                        )
                    )
                    tasks.append(
                        pool.submit(
                            login_with_invalid_username_and_valid_password,
                            (
                                users,
                                i,
                                50,
                            ),
                        )
                    )
        finally:
            with Then("it should work"):
                for task in tasks:
                    task.result(timeout=timeout)


@TestScenario
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_Authentication_Parallel("1.0"),
    RQ_SRS_014_LDAP_RoleMapping_Authentication_Parallel_ValidAndInvalid("1.0"),
)
def parallel_login_of_multiple_users(
    self, ldap_server, ldap_user, timeout=200, role_count=10
):
    """Check that valid and invalid logins of multiple LDAP authenticated users
    with mapped roles works in parallel.
    """
    parallel_login(
        user_count=10,
        ldap_user=ldap_user,
        ldap_server=ldap_server,
        timeout=timeout,
        role_count=role_count,
    )


@TestScenario
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_Authentication_Parallel_SameUser("1.0"),
    RQ_SRS_014_LDAP_RoleMapping_Authentication_Parallel_ValidAndInvalid("1.0"),
)
def parallel_login_of_the_same_user(
    self, ldap_server, ldap_user, timeout=200, role_count=10
):
    """Check that valid and invalid logins of the same LDAP authenticated user
    with mapped roles works in parallel.
    """
    parallel_login(
        user_count=10,
        ldap_user=ldap_user,
        ldap_server=ldap_server,
        timeout=timeout,
        role_count=role_count,
    )


@TestScenario
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_Authentication_Parallel_MultipleServers("1.0"),
    RQ_SRS_014_LDAP_RoleMapping_Authentication_Parallel_ValidAndInvalid("1.0"),
)
def parallel_login_of_ldap_users_with_multiple_servers(
    self, ldap_server, ldap_user, timeout=200
):
    """Check that valid and invalid logins of multiple LDAP users that have mapped roles
    works in parallel using multiple LDAP external user directories.
    """
    parallel_login_with_multiple_servers(
        ldap_server=ldap_server,
        ldap_user=ldap_user,
        user_count=10,
        role_count=10,
        timeout=timeout,
        with_ldap_users=True,
        with_local_users=False,
    )


@TestScenario
@Requirements(
    RQ_SRS_014_LDAP_RoleMapping_Authentication_Parallel_LocalAndMultipleLDAP("1.0"),
    RQ_SRS_014_LDAP_RoleMapping_Authentication_Parallel_ValidAndInvalid("1.0"),
)
def parallel_login_of_local_and_ldap_users_with_multiple_servers(
    self, ldap_server, ldap_user, timeout=200
):
    """Check that valid and invalid logins of local users and LDAP users that have mapped roles
    works in parallel using multiple LDAP external user directories.
    """
    parallel_login_with_multiple_servers(
        ldap_server=ldap_server,
        ldap_user=ldap_user,
        user_count=10,
        role_count=10,
        timeout=timeout,
        with_local_users=True,
        with_ldap_users=True,
    )


@TestScenario
@Requirements(RQ_SRS_014_LDAP_RoleMapping_Authentication_Parallel_LocalOnly("1.0"))
def parallel_login_of_local_users(self, ldap_server, ldap_user, timeout=200):
    """Check that valid and invalid logins of local users
    works in parallel when multiple LDAP external user directories
    with role mapping are configured.
    """
    parallel_login_with_multiple_servers(
        ldap_server=ldap_server,
        ldap_user=ldap_user,
        user_count=10,
        role_count=10,
        timeout=timeout,
        with_local_users=True,
        with_ldap_users=False,
    )


@TestOutline
def parallel_login_with_multiple_servers(
    self,
    ldap_server,
    ldap_user,
    user_count=10,
    role_count=10,
    timeout=200,
    with_ldap_users=True,
    with_local_users=False,
):
    """Check that login of valid and invalid local users or LDAP users that have mapped roles
    works in parallel using multiple LDAP external user directories.
    """
    uid = getuid()

    cluster = self.context.cluster
    user_groups = {}

    with Given("I define role names"):
        role_names = [f"role{i}_{uid}" for i in range(role_count)]

    with And("I define corresponding group names"):
        groups = [{"cn": f"clickhouse_{role_name}"} for role_name in role_names]

    if with_ldap_users:
        with And("I define a group of users to be created on each LDAP server"):
            user_groups["openldap1_users"] = [
                {
                    "cn": f"openldap1_parallel_user{i}_{uid}",
                    "userpassword": randomword(20),
                }
                for i in range(user_count)
            ]
            user_groups["openldap2_users"] = [
                {
                    "cn": f"openldap2_parallel_user{i}_{uid}",
                    "userpassword": randomword(20),
                }
                for i in range(user_count)
            ]

    if with_local_users:
        with And("I define a group of local users to be created"):
            user_groups["local_users"] = [
                {"cn": f"local_parallel_user{i}_{uid}", "userpassword": randomword(20)}
                for i in range(user_count)
            ]

    with And("I have a list of checks that I want to run for each user group"):
        checks = [
            login_with_valid_username_and_password,
            login_with_valid_username_and_invalid_password,
            login_with_invalid_username_and_valid_password,
        ]

    with And(
        "I create config file to define LDAP external user directory for each LDAP server"
    ):
        entries = {
            "user_directories": [
                {
                    "ldap": [
                        {"server": "openldap1"},
                        {
                            "role_mappings": [
                                {
                                    "base_dn": "ou=groups,dc=company,dc=com",
                                    "attribute": "cn",
                                    "search_filter": "(&(objectClass=groupOfUniqueNames)(uniquemember={bind_dn}))",
                                    "prefix": "clickhouse_",
                                }
                            ]
                        },
                    ]
                },
                {
                    "ldap": [
                        {"server": "openldap2"},
                        {
                            "role_mappings": [
                                {
                                    "base_dn": "ou=groups,dc=company,dc=com",
                                    "attribute": "cn",
                                    "search_filter": "(&(objectClass=groupOfUniqueNames)(uniquemember={bind_dn}))",
                                    "prefix": "clickhouse_",
                                }
                            ]
                        },
                    ]
                },
            ]
        }
        config = create_entries_ldap_external_user_directory_config_content(entries)

    with And("I add LDAP external user directory configuration"):
        add_ldap_external_user_directory(server=None, restart=True, config=config)

    if with_ldap_users:
        with And("I add LDAP users to each LDAP server"):
            openldap1_users = add_ldap_users(
                users=user_groups["openldap1_users"], node=cluster.node("openldap1")
            )
            openldap2_users = add_ldap_users(
                users=user_groups["openldap2_users"], node=cluster.node("openldap2")
            )

        with And("I add all LDAP groups to each LDAP server"):
            openldap1_groups = add_ldap_groups(
                groups=groups, node=cluster.node("openldap1")
            )
            openldap2_groups = add_ldap_groups(
                groups=groups, node=cluster.node("openldap2")
            )

        with And("I add all users to LDAP groups on the first LDAP server"):
            for group in openldap1_groups:
                for user in openldap1_users:
                    with By(
                        f"adding LDAP user {user['dn']} to the group {group['dn']}"
                    ):
                        add_user_to_group_in_ldap(
                            user=user, group=group, node=cluster.node("openldap1")
                        )

        with And("I add all users to LDAP groups on the second LDAP server"):
            for group in openldap2_groups:
                for user in openldap2_users:
                    with By(
                        f"adding LDAP user {user['dn']} to the group {group['dn']}"
                    ):
                        add_user_to_group_in_ldap(
                            user=user, group=group, node=cluster.node("openldap2")
                        )

    with And("I add RBAC roles"):
        add_rbac_roles(roles=role_names)

    if with_local_users:
        with And("I add local users"):
            add_rbac_users(users=user_groups["local_users"])

        with And("I grant the same RBAC roles to local users"):
            for user in user_groups["local_users"]:
                for role_name in role_names:
                    self.context.node.query(f"GRANT {role_name} TO {user['cn']}")

    tasks = []
    with Pool(4) as pool:
        try:
            with When(
                "users in each group try to login in parallel",
                description="""
                * with valid username and password
                * with invalid username and valid password
                * with valid username and invalid password
                """,
            ):
                for i in range(10):
                    for users in user_groups.values():
                        for check in checks:
                            tasks.append(
                                pool.submit(
                                    check,
                                    (
                                        users,
                                        i,
                                        50,
                                    ),
                                )
                            )
        finally:
            with Then("it should work"):
                for task in tasks:
                    task.result(timeout=timeout)


@TestFeature
@Name("mapping")
@Requirements(RQ_SRS_014_LDAP_RoleMapping_Search("1.0"))
def feature(self):
    """Check role LDAP role mapping."""
    self.context.node = self.context.cluster.node("clickhouse1")
    self.context.ldap_node = self.context.cluster.node("openldap1")

    servers = {
        "openldap1": {
            "host": "openldap1",
            "port": "389",
            "enable_tls": "no",
            "bind_dn": "cn={user_name},ou=users,dc=company,dc=com",
        },
        "openldap2": {
            "host": "openldap2",
            "port": "636",
            "enable_tls": "yes",
            "bind_dn": "cn={user_name},ou=users,dc=company,dc=com",
            "tls_require_cert": "never",
        },
    }

    users = [
        {
            "server": "openldap1",
            "username": "user1",
            "password": "user1",
            "login": True,
            "dn": "cn=user1,ou=users,dc=company,dc=com",
        },
    ]

    with Given("I fix LDAP access permissions"):
        fix_ldap_permissions()

    with And("I add LDAP servers configuration", description=f"{servers}"):
        add_ldap_servers_configuration(servers=servers)

    for scenario in loads(current_module(), Scenario):
        scenario(ldap_server="openldap1", ldap_user=users[0])
