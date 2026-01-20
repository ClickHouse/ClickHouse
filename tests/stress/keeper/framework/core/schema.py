def validate_scenario(s):
    errs = []
    if not isinstance(s, dict):
        return ["scenario_not_dict"]
    sid = s.get("id")
    if not isinstance(sid, str) or not sid.strip():
        errs.append("missing_id")
    if "duration" in s:
        try:
            int(s.get("duration"))
        except Exception:
            errs.append("duration_not_int")
    if "topology" in s:
        try:
            int(s.get("topology"))
        except Exception:
            errs.append("topology_not_int")
    if "backend" in s and not isinstance(s.get("backend"), str):
        errs.append("backend_not_str")
    if "faults" in s and not isinstance(s.get("faults"), list):
        errs.append("faults_not_list")
    if "post" in s and not isinstance(s.get("post"), list):
        errs.append("post_not_list")
    if "gates" in s and not isinstance(s.get("gates"), list):
        errs.append("gates_not_list")
    if "workload" in s:
        if not isinstance(s.get("workload"), dict):
            errs.append("workload_not_dict")
        else:
            if "duration" in s.get("workload"):
                errs.append("workload_duration_not_supported")
    return errs
