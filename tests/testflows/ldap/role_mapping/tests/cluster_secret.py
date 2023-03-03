from testflows.core import *
from testflows.asserts import error

from ldap.role_mapping.requirements import *
from ldap.role_mapping.tests.common import *


def cluster_node(name):
    """Get cluster node instance."""
    return current().context.cluster.node(name)


@TestStep(Given)
def add_sharded_cluster(
    self, node, name="sharded_cluster_with_secret", with_secret=True, restart=False
):
    """Add configuration of sharded cluster that uses secret."""
    entries = {"remote_servers": {name: []}}

    if with_secret:
        entries["remote_servers"][name].append({"secret": "qwerty123"})

    for node_name in self.context.cluster.nodes["clickhouse"]:
        entries["remote_servers"][name].append(
            {
                "shard": {"replica": {"host": node_name, "port": "9000"}},
            },
        )

    config = create_xml_config_content(entries=entries, config_file=f"{name}.xml")
    return add_config(config, node=node, restart=restart)


@TestStep(Given)
def create_table(self, on_cluster, name=None, node=None):
    """Create table on cluster."""
    if node is None:
        node = self.context.node
    if name is None:
        name = getuid()

    try:
        node.query(
            f"CREATE TABLE {name} ON CLUSTER {on_cluster} (d Date, a String, b UInt8, x String, y Int8) "
            f"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{name}', '{{replica}}') "
            "PARTITION BY y ORDER BY (d, b)"
        )
        yield name
    finally:
        with Finally(f"I drop table {name} on cluster {on_cluster} on {node.name}"):
            node.query(f"DROP TABLE IF EXISTS {name} ON CLUSTER {on_cluster} SYNC")


@TestStep(Given)
def create_distributed_table(self, on_cluster, over, name=None, node=None):
    """Create distributed table on cluster over some underlying table."""
    if node is None:
        node = self.context.node
    if name is None:
        name = getuid()

    try:
        node.query(
            f"CREATE TABLE {name} ON CLUSTER {on_cluster} AS {over} "
            f"ENGINE = Distributed({on_cluster}, default, {over}, rand())"
        )
        yield name
    finally:
        with Finally(f"I drop table {name} on cluster {on_cluster} on {node.name}"):
            node.query(f"DROP TABLE IF EXISTS {name} ON CLUSTER {on_cluster} SYNC")


@TestStep(Given)
def grant_select(self, cluster, privilege, role_or_user, node=None):
    """Grant select privilege on a table on a given cluster
    to a role or a user.
    """
    if node is None:
        node = self.context.node

    try:
        node.query(f"GRANT ON CLUSTER {cluster} {privilege} TO {role_or_user}")
        yield
    finally:
        with Finally(
            f"I remove privilege '{privilege}' on {cluster} from {role_or_user}"
        ):
            node.query(f"REVOKE ON CLUSTER {cluster} {privilege} FROM {role_or_user}")


@TestScenario
def select_using_mapped_role(self, cluster, role_name, role_mapped, user):
    """Check accessing normal and distributed table using
    a user and the specified role that is either granted
    rights to access the tables or not and is or is not assigned to the user
    from all cluster nodes.
    """
    # default cluster node
    node = cluster_node("clickhouse1")

    query_settings = [("user", user["username"]), ("password", user["password"])]

    with Given(f"I create base table on cluster {cluster}"):
        src_table = create_table(on_cluster=cluster, node=node)

    with And(f"I create distristibuted table over base table on cluster {cluster}"):
        dist_table = create_distributed_table(
            on_cluster=cluster, over=src_table, node=node
        )

    with And("I check that grants for the user"):
        for name in self.context.cluster.nodes["clickhouse"]:
            for attempt in retries(timeout=10):
                with attempt:
                    with By(f"executing query on node {name}", flags=TE):
                        r = self.context.cluster.node(name).query(
                            f"SHOW GRANTS", settings=query_settings
                        )
                        if role_mapped:
                            with Then("check that role is mapped"):
                                assert role_name in r.output, error()

    with Example("no privilege on source table"):
        with When("user tries to read from the source table without privilege"):
            for name in self.context.cluster.nodes["clickhouse"]:
                with By(f"executing query on node {name}", flags=TE):
                    self.context.cluster.node(name).query(
                        f"SELECT * FROM {src_table}",
                        settings=query_settings,
                        exitcode=241,
                        message=f"DB::Exception:",
                    )

    with Example("with privilege on source table"):
        with Given("I grant SELECT on source table to the mapped role"):
            grant_select(
                cluster=cluster,
                privilege=f"SELECT ON {src_table}",
                role_or_user=role_name,
                node=node,
            )

        with Then("user should be able to read from the source table"):
            for name in self.context.cluster.nodes["clickhouse"]:
                with By(f"executing query on node {name}", flags=TE):
                    self.context.cluster.node(name).query(
                        f"SELECT * FROM {src_table}",
                        settings=query_settings,
                        exitcode=0 if role_mapped else 241,
                        message="" if role_mapped else "DB::Exception:",
                    )

    with Example("with privilege only on distributed table"):
        with Given("I grant SELECT on distributed table to the mapped role"):
            grant_select(
                cluster=cluster,
                privilege=f"SELECT ON {dist_table}",
                role_or_user=role_name,
                node=node,
            )

        with Then("user should still not be able to read from distributed table"):
            for name in self.context.cluster.nodes["clickhouse"]:
                with By(f"executing query on node {name}", flags=TE):
                    self.context.cluster.node(name).query(
                        f"SELECT * FROM {dist_table}",
                        settings=query_settings,
                        exitcode=241,
                        message=f"DB::Exception:",
                    )

    with Example("with privilege only on source but not on distributed table"):
        with Given("I grant SELECT on source table to the mapped role"):
            grant_select(
                cluster=cluster,
                privilege=f"SELECT ON {src_table}",
                role_or_user=role_name,
                node=node,
            )

        with Then("user should still not be able to read from distributed table"):
            for name in self.context.cluster.nodes["clickhouse"]:
                with By(f"executing query on node {name}", flags=TE):
                    self.context.cluster.node(name).query(
                        f"SELECT * FROM {dist_table}",
                        settings=query_settings,
                        exitcode=241,
                        message=f"DB::Exception:",
                    )

    with Example("with privilege on source and distributed"):
        with Given("I grant SELECT on source table to the mapped role"):
            grant_select(
                cluster=cluster,
                privilege=f"SELECT ON {src_table}",
                role_or_user=role_name,
                node=node,
            )

        with And("I grant SELECT on distributed table to the mapped role"):
            grant_select(
                cluster=cluster,
                privilege=f"SELECT ON {dist_table}",
                role_or_user=role_name,
                node=node,
            )

        with Then("user should be able to read from the distributed table"):
            for name in self.context.cluster.nodes["clickhouse"]:
                with By(f"executing query on node {name}", flags=TE):
                    self.context.cluster.node(name).query(
                        f"SELECT * FROM {dist_table}",
                        settings=query_settings,
                        exitcode=0 if role_mapped else 241,
                        message="" if role_mapped else "DB::Exception:",
                    )


@TestFeature
def execute_tests(self, role_name, role_mapped, ldap_user, local_user):
    """Execute all scenarios on cluster with or without secret
    for LDAP and local users, using a role that might be
    mapped or not.
    """
    for cluster_type in ["with secret", "without secret"]:
        with Feature("cluster " + cluster_type):
            for user in [ldap_user, local_user]:
                with Feature(user["type"]):
                    with Feature(f"role {role_name} mapped {role_mapped}"):
                        if role_mapped and user["type"] == "local user":
                            with Given(f"I grant role {role_name} to local RBAC user"):
                                for name in self.context.cluster.nodes["clickhouse"]:
                                    with By(f"on node {name}"):
                                        cluster_node(name).query(
                                            f"GRANT {role_name} TO {local_user['username']}"
                                        )

                        for scenario in ordered(loads(current_module(), Scenario)):
                            scenario(
                                cluster="sharded_cluster_"
                                + cluster_type.replace(" ", "_"),
                                role_name=role_name,
                                role_mapped=role_mapped,
                                user=user,
                            )


@TestOutline(Feature)
def outline_using_external_user_directory(
    self, ldap_servers, mapping, ldap_roles_or_groups, rbac_roles, mapped_roles
):
    """Check using simple and distributed table access when using
    LDAP external user directory or LDAP authenticated existing RBAC users
    with and without cluster secret.

    Where mapping can be one of the following:
        'static' or 'dynamic' or 'dynamic and static'
    """
    ldap_user = {
        "type": "ldap user",
        "server": "openldap1",
        "username": "user1",
        "password": "user1",
        "dn": "cn=user1,ou=users,dc=company,dc=com",
    }

    local_user = {
        "type": "local user",
        "username": "local_user1",
        "password": "local_user1",
    }

    role_mappings = [
        {
            "base_dn": "ou=groups,dc=company,dc=com",
            "attribute": "cn",
            "search_filter": "(&(objectClass=groupOfUniqueNames)(uniquemember={bind_dn}))",
            "prefix": "clickhouse_",
        }
    ]

    if mapping in ["dynamic", "dynamic and static"]:
        with Given("I add LDAP groups"):
            for name in ldap_servers:
                for group_name in ldap_roles_or_groups:
                    with By(f"adding {group_name}"):
                        ldap_groups = add_ldap_groups(
                            groups=({"cn": group_name},), node=cluster_node(name)
                        )

                    with And("I add LDAP user to the group"):
                        add_user_to_group_in_ldap(
                            user=ldap_user,
                            group=ldap_groups[0],
                            node=cluster_node(name),
                        )

    with Given(
        f"I add LDAP external user directory configuration with {mapping} role mapping"
    ):
        for name in self.context.cluster.nodes["clickhouse"]:
            if mapping == "dynamic":
                By(
                    f"on node {name}",
                    test=add_ldap_external_user_directory,
                    parallel=True,
                )(
                    server="openldap1",
                    role_mappings=role_mappings,
                    restart=True,
                    node=cluster_node(name),
                )
            elif mapping == "dynamic and static":
                By(
                    f"on node {name}",
                    test=add_ldap_external_user_directory,
                    parallel=True,
                )(
                    server="openldap1",
                    role_mappings=role_mappings,
                    roles=ldap_roles_or_groups,
                    restart=True,
                    node=cluster_node(name),
                )
            else:
                By(
                    f"on node {name}",
                    test=add_ldap_external_user_directory,
                    parallel=True,
                )(
                    server="openldap1",
                    roles=ldap_roles_or_groups,
                    restart=True,
                    node=cluster_node(name),
                )

    with And("I add local RBAC user"):
        for name in self.context.cluster.nodes["clickhouse"]:
            with By(f"on node {name}"):
                add_rbac_users(users=[local_user], node=cluster_node(name))

    with And("I add RBAC roles on cluster"):
        for name in self.context.cluster.nodes["clickhouse"]:
            with By(f"on node {name}"):
                add_rbac_roles(roles=rbac_roles, node=cluster_node(name))

    for role_name in rbac_roles:
        execute_tests(
            role_name=role_name,
            role_mapped=(role_name in mapped_roles),
            ldap_user=ldap_user,
            local_user=local_user,
        )


@TestFeature
def using_authenticated_users(self, ldap_servers):
    """Check using simple and distributed table access when using
    LDAP authenticated existing users with and without cluster secret.
    """
    role_name = f"role_{getuid()}"

    ldap_user = {
        "type": "ldap authenticated user",
        "cn": "myuser",
        "username": "myuser",
        "userpassword": "myuser",
        "password": "myuser",
        "server": "openldap1",
    }

    local_user = {
        "type": "local user",
        "username": "local_user2",
        "password": "local_user2",
    }

    with Given("I add LDAP user"):
        add_user = {
            "cn": ldap_user["cn"],
            "userpassword": ldap_user["userpassword"],
        }
        for name in ldap_servers:
            add_ldap_users(users=[add_user], node=cluster_node(name))

    with And("I add LDAP authenticated users configuration"):
        for name in self.context.cluster.nodes["clickhouse"]:
            By(f"on node {name}", test=add_ldap_authenticated_users, parallel=True)(
                users=[ldap_user], rbac=True, node=cluster_node(name)
            )

    with And("I add local RBAC user"):
        for name in self.context.cluster.nodes["clickhouse"]:
            with By(f"on node {name}"):
                add_rbac_users(users=[local_user], node=cluster_node(name))

    with And("I add RBAC role on cluster that user will use"):
        for name in self.context.cluster.nodes["clickhouse"]:
            with By(f"on node {name}"):
                add_rbac_roles(roles=(f"{role_name}",), node=cluster_node(name))

    with And("I grant role to LDAP authenticated user"):
        for name in self.context.cluster.nodes["clickhouse"]:
            with By(f"on node {name}"):
                cluster_node(name).query(
                    f"GRANT {role_name} TO {ldap_user['username']}"
                )

    with And("I grant role to local RBAC user"):
        for name in self.context.cluster.nodes["clickhouse"]:
            with By(f"on node {name}"):
                cluster_node(name).query(
                    f"GRANT {role_name} TO {local_user['username']}"
                )

    execute_tests(
        role_name=role_name,
        role_mapped=role_name,
        ldap_user=ldap_user,
        local_user=local_user,
    )


@TestFeature
def using_external_user_directory(self, ldap_servers):
    """Check using LDAP external user directory with different
    role mapping mode and different cases of role existens.
    """
    uid = getuid()

    for mapping in ["dynamic", "static", "dynamic and static"]:
        with Example(f"{mapping}"):
            with Example("all mapped roles exist"):
                if mapping == "dynamic":
                    ldap_roles_or_groups = [
                        f"clickhouse_role0_{uid}",
                        f"clickhouse_role1_{uid}",
                    ]
                elif mapping == "dynamic and static":
                    ldap_roles_or_groups = [
                        f"clickhouse_role0_{uid}",
                        f"clickhouse_role1_{uid}",
                        f"role2_{uid}",
                        f"role3_{uid}",
                    ]
                else:
                    ldap_roles_or_groups = [
                        f"role0_{uid}",
                        f"role1_{uid}",
                        f"role2_{uid}",
                        f"role3_{uid}",
                    ]

                rbac_roles = [f"role0_{uid}", f"role1_{uid}"]
                mapped_roles = [f"role0_{uid}", f"role1_{uid}"]

                outline_using_external_user_directory(
                    ldap_servers=ldap_servers,
                    mapping=mapping,
                    ldap_roles_or_groups=ldap_roles_or_groups,
                    rbac_roles=rbac_roles,
                    mapped_roles=mapped_roles,
                )

            with Example("some mapped roles exist"):
                if mapping == "dynamic":
                    ldap_roles_or_groups = [
                        f"clickhouse_role0_{uid}",
                        f"clickhouse_role1_{uid}",
                    ]
                elif mapping == "dynamic and static":
                    ldap_roles_or_groups = [
                        f"clickhouse_role0_{uid}",
                        f"clickhouse_role1_{uid}",
                        f"role2_{uid}",
                        f"role3_{uid}",
                    ]
                else:
                    ldap_roles_or_groups = [f"role0_{uid}", f"role1_{uid}"]

                rbac_roles = [f"role0_{uid}", f"role_not_mapped_{uid}", f"role2_{uid}"]

                if mapping == "dynamic and static":
                    mapped_roles = [f"role0_{uid}", f"role2_{uid}"]
                else:
                    mapped_roles = [f"role0_{uid}"]

                outline_using_external_user_directory(
                    ldap_servers=ldap_servers,
                    mapping=mapping,
                    ldap_roles_or_groups=ldap_roles_or_groups,
                    rbac_roles=rbac_roles,
                    mapped_roles=mapped_roles,
                )

            with Example("no mapped roles exist"):
                if mapping == "dynamic":
                    ldap_roles_or_groups = [
                        f"clickhouse_role0_{uid}",
                        f"clickhouse_role1_{uid}",
                    ]
                elif mapping == "dynamic and static":
                    ldap_roles_or_groups = [
                        f"clickhouse_role0_{uid}",
                        f"clickhouse_role1_{uid}",
                        f"role2_{uid}",
                        f"role3_{uid}",
                    ]
                else:
                    ldap_roles_or_groups = [f"role0_{uid}", f"role1_{uid}"]

                rbac_roles = [f"role_not_mapped0_{uid}", f"role_not_mapped1_{uid}"]
                mapped_roles = []

                outline_using_external_user_directory(
                    ldap_servers=ldap_servers,
                    mapping=mapping,
                    ldap_roles_or_groups=ldap_roles_or_groups,
                    rbac_roles=rbac_roles,
                    mapped_roles=mapped_roles,
                )

            with Example("empty roles"):
                ldap_roles_or_groups = []
                rbac_roles = [f"role0_{uid}", f"role1_{uid}"]
                mapped_roles = []

                outline_using_external_user_directory(
                    ldap_servers=ldap_servers,
                    mapping=mapping,
                    ldap_roles_or_groups=ldap_roles_or_groups,
                    rbac_roles=rbac_roles,
                    mapped_roles=mapped_roles,
                )


@TestFeature
@Name("cluster secret")
@Requirements(RQ_SRS_014_LDAP_ClusterWithAndWithoutSecret_DistributedTable("1.0"))
def feature(self):
    """Check using Distributed table when cluster is configured with and without secret
    using users authenticated via LDAP either through external user directory
    or defined using RBAC with LDAP server authentication.
    """
    ldap_servers = {
        "openldap1": {
            "host": "openldap1",
            "port": "389",
            "enable_tls": "no",
            "bind_dn": "cn={user_name},ou=users,dc=company,dc=com",
        },
    }

    with Given("I fix LDAP access permissions"):
        for name in ldap_servers:
            fix_ldap_permissions(node=cluster_node(name))

    with And(
        "I add LDAP servers configuration on all nodes", description=f"{ldap_servers}"
    ):
        for name in self.context.cluster.nodes["clickhouse"]:
            By(f"on node {name}", test=add_ldap_servers_configuration, parallel=True)(
                servers=ldap_servers, node=cluster_node(name)
            )

    with And("I add sharded cluster that uses secrets on all the nodes"):
        for name in self.context.cluster.nodes["clickhouse"]:
            By(
                f"adding configuration on {name}",
                test=add_sharded_cluster,
                parallel=True,
            )(
                node=cluster_node(name),
                name="sharded_cluster_with_secret",
                with_secret=True,
            )

    with And("I add sharded cluster that does not use secrets on all the nodes"):
        for name in self.context.cluster.nodes["clickhouse"]:
            By(
                f"adding configuration on {name}",
                test=add_sharded_cluster,
                parallel=True,
            )(
                node=cluster_node(name),
                name="sharded_cluster_without_secret",
                with_secret=False,
            )

    Feature("external user directory", test=using_external_user_directory)(
        ldap_servers=ldap_servers
    )
    Feature("authenticated users", test=using_authenticated_users)(
        ldap_servers=ldap_servers
    )
