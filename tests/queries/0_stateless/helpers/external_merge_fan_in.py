import os
import re
import shlex
import subprocess
from pathlib import Path

root = Path(os.environ["LOCAL_DIR"])
command = shlex.split(os.environ["CLICKHOUSE_LOCAL"]) + [
    "--path",
    str(root / "data"),
    "--max_threads=1",
    "--max_block_size=4096",
    "--max_bytes_before_external_sort=1",
    "--max_bytes_ratio_before_external_sort=0",
    "--max_bytes_before_external_distinct=1",
    "--max_bytes_ratio_before_external_distinct=0",
    "--max_untracked_memory=0",
    "--optimize_distinct_in_order=0",
    "--allow_preliminary_distinct_abandoning=0",
    "--query_plan_remove_redundant_sorting=0",
    "--logger.console",
    "--logger.level=trace",
    "--print-profile-events",
]


def check(
    name, sql, expected, fan_in=2, intermediate=True, extra=(), error=None, final=True
):
    log = root / (name + ".log")
    with log.open("w") as stderr:
        overridden = {option.split("=", 1)[0] for option in extra}
        options = [
            option for option in command if option.split("=", 1)[0] not in overridden
        ]
        result = subprocess.run(
            options
            + [f"--max_external_merge_fan_in={fan_in}", "--query", sql]
            + list(extra),
            stdout=subprocess.PIPE,
            stderr=stderr,
            text=True,
            timeout=180,
        )
    text = log.read_text()
    if error is None:
        assert result.returncode == 0, (name, text)
        assert result.stdout.strip() == expected, (name, result.stdout, expected)
    else:
        assert result.returncode != 0 and error in text, (name, text)
    groups = [
        int(n)
        for n in re.findall(
            r"Starting intermediate external merge with (\d+) inputs", text
        )
    ]
    finals = [
        int(n)
        for n in re.findall(r"Starting final external merge with (\d+) files", text)
    ]
    assert bool(groups) == intermediate, (name, groups, text[-10000:])
    assert bool(finals) == final and all(n <= fan_in for n in finals), (name, finals)
    assert all(2 <= n <= fan_in for n in groups), (name, groups)
    if error is None:

        # Completed merges count file inputs, while the direct path leaves both counters at zero.
        completed = [
            int(n)
            for n in re.findall(
                r"Finished intermediate external merge of (\d+) inputs", text
            )
        ]
        for event, expected_value in (
            ("ExternalProcessingIntermediateMerge", len(completed)),
            ("ExternalProcessingIntermediateMergeInputs", sum(completed)),
        ):
            value = sum(
                int(n) for n in re.findall(rf"{event}: (\d+) \(increment\)", text)
            )
            assert value == expected_value, (name, event, value, expected_value)
        assert len(completed) == len(groups), (name, completed, groups)
    assert not list((root / "data" / "tmp").glob("tmp*")), name
    print(name, "ok", flush=True)
    return groups, text
