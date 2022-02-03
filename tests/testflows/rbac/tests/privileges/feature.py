from testflows.core import *

from rbac.helper.common import *

@TestFeature
@Name("privileges")
def feature(self):

    tasks = []
    with Pool(10) as pool:
        try:
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.insert", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.select", "feature"), ), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.public_tables", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.distributed_table", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.grant_option", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.truncate", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.optimize", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.kill_query", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.kill_mutation", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.role_admin", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.dictGet", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.introspection", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.sources", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.admin_option", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.all_role", "feature")), {})

            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.show.show_tables", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.show.show_dictionaries", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.show.show_databases", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.show.show_columns", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.show.show_users", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.show.show_roles", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.show.show_quotas", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.show.show_settings_profiles", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.show.show_row_policies", "feature")), {})

            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_column", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_index", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_constraint", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_ttl", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_settings", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_update", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_delete", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_freeze", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_fetch", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_move", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_user", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_role", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_row_policy", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_quota", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.alter.alter_settings_profile", "feature")), {})

            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.create.create_database", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.create.create_dictionary", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.create.create_temp_table", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.create.create_table", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.create.create_user", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.create.create_role", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.create.create_row_policy", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.create.create_quota", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.create.create_settings_profile", "feature")), {})

            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.attach.attach_database", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.attach.attach_dictionary", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.attach.attach_temp_table", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.attach.attach_table", "feature")), {})

            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.drop.drop_database", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.drop.drop_dictionary", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.drop.drop_table", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.drop.drop_user", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.drop.drop_role", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.drop.drop_row_policy", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.drop.drop_quota", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.drop.drop_settings_profile", "feature")), {})

            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.detach.detach_database", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.detach.detach_dictionary", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.detach.detach_table", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.detach.detach_view", "feature")), {})

            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.system.drop_cache", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.system.reload", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.system.flush", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.system.merges", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.system.moves", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.system.replication_queues", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.system.ttl_merges", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.system.restart_replica", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.system.sends", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.system.sync_replica", "feature")), {})
            run_scenario(pool, tasks, Feature(test=load("rbac.tests.privileges.system.fetches", "feature")), {})

        finally:
            join(tasks)

    Feature(test=load("rbac.tests.privileges.system.shutdown", "feature"))
