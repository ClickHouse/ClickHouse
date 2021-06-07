## Syntax

# Errors: not found

not_found = "Exception: There is no {type} `{name}`"

def user_not_found_in_disk(name):
    return (192,not_found.format(type="user",name=name))

def role_not_found_in_disk(name):
    return (255,not_found.format(type="role",name=name))

def settings_profile_not_found_in_disk(name):
    return (180,not_found.format(type="settings profile",name=name))

def quota_not_found_in_disk(name):
    return (199,not_found.format(type="quota",name=name))

def row_policy_not_found_in_disk(name):
    return (11,not_found.format(type="row policy",name=name))

def table_does_not_exist(name):
    return(60,"Exception: Table {name} doesn't exist".format(name=name))

# Errors: cannot_rename

cannot_rename = "Exception: {type} `{name}`: cannot rename to `{name_new}` because {type} `{name_new}` already exists"
cannot_rename_exitcode = 237

def cannot_rename_user(name,name_new):
    return (cannot_rename_exitcode, cannot_rename.format(type="user", name=name, name_new=name_new))

def cannot_rename_role(name,name_new):
    return (cannot_rename_exitcode, cannot_rename.format(type="role", name=name, name_new=name_new))

def cannot_rename_settings_profile(name,name_new):
    return (cannot_rename_exitcode, cannot_rename.format(type="settings profile", name=name, name_new=name_new))

def cannot_rename_quota(name,name_new):
    return (cannot_rename_exitcode, cannot_rename.format(type="quota", name=name, name_new=name_new))

def cannot_rename_row_policy(name,name_new):
    return (cannot_rename_exitcode, cannot_rename.format(type="row policy", name=name, name_new=name_new))

# Errors: cannot insert

cannot_insert = "Exception: {type} `{name}`: cannot insert because {type} `{name}` already exists"
cannot_insert_exitcode = 237

def cannot_insert_user(name):
    return (cannot_insert_exitcode, cannot_insert.format(type="user",name=name))

def cannot_insert_role(name):
    return (cannot_insert_exitcode, cannot_insert.format(type="role",name=name))

def cannot_insert_settings_profile(name):
    return (cannot_insert_exitcode, cannot_insert.format(type="settings profile",name=name))

def cannot_insert_quota(name):
    return (cannot_insert_exitcode, cannot_insert.format(type="quota",name=name))

def cannot_insert_row_policy(name):
    return (cannot_insert_exitcode, cannot_insert.format(type="row policy",name=name))

# Error: default is readonly

cannot_remove_default = "Exception: Cannot remove {type} `default` from users.xml because this storage is readonly"
cannot_remove_default_exitcode = 239

def cannot_update_default():
    return (cannot_remove_default_exitcode, "Exception: Cannot update user `default` in users.xml because this storage is readonly")

def cannot_remove_user_default():
    return (cannot_remove_default_exitcode, cannot_remove_default.format(type="user"))

def cannot_remove_settings_profile_default():
    return (cannot_remove_default_exitcode, cannot_remove_default.format(type="settings profile"))

def cannot_remove_quota_default():
    return (cannot_remove_default_exitcode, cannot_remove_default.format(type="quota"))

# Other syntax errors

def unknown_setting(setting):
    return (115, f"Exception: Unknown setting {setting}.")

def cluster_not_found(cluster):
    return (170, f"Exception: Requested cluster '{cluster}' not found.")

## Privileges

def not_enough_privileges(name):
    return (241, f"Exception: {name}: Not enough privileges.")

def cannot_parse_string_as_float(string):
    return (6, f"Exception: Cannot parse string '{string}' as Float64")

def missing_columns(name):
    return (47, f"Exception: Missing columns: '{name}' while processing query")

# Errors: wrong name

wrong_name = "Exception: Wrong {type} name. Cannot find {type} `{name}` to drop"

def wrong_column_name(name):
    return (10, wrong_name.format(type="column",name=name))

def wrong_index_name(name):
    return (36, wrong_name.format(type="index",name=name))

def wrong_constraint_name(name):
    return (36, wrong_name.format(type="constraint",name=name))

# Errors: cannot add

cannot_add = "Exception: Cannot add index {name}: index with this name already exists"
cannot_add_exitcode = 44

def cannot_add_index(name):
    return (cannot_add_exitcode, cannot_add.format(name=name))

def cannot_add_constraint(name):
    return (cannot_add_exitcode, cannot_add.format(name=name))
