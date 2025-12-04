from .core.scenario_builder import ScenarioBuilder


def build_gp3_jitter(
    sid="CHA-GP3-JIT",
    name="Network jitter + gp3-like disk",
    topology=3,
    duration_s=180,
    delay_ms=10,
    jitter_ms=5,
    loss_pct=0,
    disk_ms=3,
    backend="default",
):
    sb = ScenarioBuilder(sid, name, topology=topology, backend=backend)
    sb.set_workload_config("workloads/prod_mix.yaml", duration=duration_s)
    from .core.scenario_builder import with_jitter, with_gp3_disk
    with_jitter(sb, delay_ms=delay_ms, jitter_ms=jitter_ms, loss_pct=loss_pct, duration_s=duration_s, target_parallel=True)
    with_gp3_disk(sb, ms=disk_ms, duration_s=duration_s, target_parallel=True)
    sb.gate({"type": "single_leader"})
    sb.gate({"type": "error_rate_le"})
    sb.gate({"type": "p99_le"})
    sb.gate({"type": "log_sanity_ok"})
    return sb.build()


def build_partition_during_reconfig(
    sid="RCFG-PART-NEG",
    name="Negative reconfig under partition",
    topology=3,
    backend="default",
):
    sb = ScenarioBuilder(sid, name, topology=topology, backend=backend)
    sb.set_workload_config("workloads/prod_mix.yaml", duration=120)
    sb.during(
        "partition_symmetric_during",
        "leader",
        [
            {"kind": "reconfig", "on": "leader", "operation": "remove", "server_id": 3, "ok": False}
        ],
    )
    sb.gate({"type": "single_leader"})
    sb.gate({"type": "config_converged", "timeout_s": 60})
    sb.gate({"type": "log_sanity_ok"})
    return sb.build()


 
