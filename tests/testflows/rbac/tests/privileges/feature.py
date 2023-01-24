from testflows.core import *

from rbac.helper.common import *


@TestFeature
@Name("privileges")
def feature(self):
    """Check RBAC privileges."""
    with Pool(10) as pool:
        try:
            Feature(
                run=load("rbac.tests.privileges.insert", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.select", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.public_tables", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.distributed_table", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.grant_option", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.truncate", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.optimize", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.kill_query", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.kill_mutation", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.role_admin", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.dictGet", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.introspection", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.sources", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.admin_option", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.all_role", "feature"),
                parallel=True,
                executor=pool,
            )

            Feature(
                run=load("rbac.tests.privileges.show.show_tables", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.show.show_dictionaries", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.show.show_databases", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.show.show_columns", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.show.show_users", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.show.show_roles", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.show.show_quotas", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load(
                    "rbac.tests.privileges.show.show_settings_profiles", "feature"
                ),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.show.show_row_policies", "feature"),
                parallel=True,
                executor=pool,
            )

            Feature(
                run=load("rbac.tests.privileges.alter.alter_column", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.alter.alter_index", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.alter.alter_constraint", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.alter.alter_ttl", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.alter.alter_settings", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.alter.alter_update", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.alter.alter_delete", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.alter.alter_freeze", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.alter.alter_fetch", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.alter.alter_move", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.alter.alter_user", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.alter.alter_role", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.alter.alter_row_policy", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.alter.alter_quota", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load(
                    "rbac.tests.privileges.alter.alter_settings_profile", "feature"
                ),
                parallel=True,
                executor=pool,
            )

            Feature(
                run=load("rbac.tests.privileges.create.create_database", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.create.create_dictionary", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.create.create_temp_table", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.create.create_table", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.create.create_user", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.create.create_role", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.create.create_row_policy", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.create.create_quota", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load(
                    "rbac.tests.privileges.create.create_settings_profile", "feature"
                ),
                parallel=True,
                executor=pool,
            )

            Feature(
                run=load("rbac.tests.privileges.attach.attach_database", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.attach.attach_dictionary", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.attach.attach_temp_table", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.attach.attach_table", "feature"),
                parallel=True,
                executor=pool,
            )

            Feature(
                run=load("rbac.tests.privileges.drop.drop_database", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.drop.drop_dictionary", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.drop.drop_table", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.drop.drop_user", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.drop.drop_role", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.drop.drop_row_policy", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.drop.drop_quota", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.drop.drop_settings_profile", "feature"),
                parallel=True,
                executor=pool,
            )

            Feature(
                run=load("rbac.tests.privileges.detach.detach_database", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.detach.detach_dictionary", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.detach.detach_table", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.detach.detach_view", "feature"),
                parallel=True,
                executor=pool,
            )

            Feature(
                run=load("rbac.tests.privileges.system.drop_cache", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.system.reload", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.system.flush", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.system.merges", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.system.moves", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.system.replication_queues", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.system.ttl_merges", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.system.restart_replica", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.system.sends", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.system.sync_replica", "feature"),
                parallel=True,
                executor=pool,
            )
            Feature(
                run=load("rbac.tests.privileges.system.fetches", "feature"),
                parallel=True,
                executor=pool,
            )

        finally:
            join()

    Feature(test=load("rbac.tests.privileges.system.shutdown", "feature"))
