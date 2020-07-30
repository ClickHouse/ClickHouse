## Syntax

# Errors: not found

not_found = "Exception: There is no {type} `{name}` in [disk, users.xml]"

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

# Errors: cannot_rename

cannot_rename = "Exception: {type} `{name}`: cannot rename to `{name_new}` because {type} `{name_new}` already exists in [disk]"
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

cannot_insert = "Exception: {type} `{name}`: cannot insert because {type} `{name}` already exists in [disk]"
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

default_readonly_exitcode = 239
cannot_remove_default = "Exception: Cannot remove {type} `default` from [users.xml] because this storage is readonly"

def cannot_update_default():
    return (default_readonly_exitcode, "Exception: Cannot update user `default` in [users.xml] because this storage is readonly")

def cannot_remove_user_default():
    return (default_readonly_exitcode, cannot_remove_default.format(type="user"))

def cannot_remove_settings_profile_default():
    return (default_readonly_exitcode, cannot_remove_default.format(type="settings profile"))

def cannot_remove_quota_default():
    return (default_readonly_exitcode, cannot_remove_default.format(type="quota"))

# Other syntax errors

def unknown_setting(setting):
    return (115, f"Exception: Unknown setting {setting}")

def cluster_not_found(cluster):
    return (170, f"Exception: Requested cluster '{cluster}' not found")

