#!/usr/bin/env python3

from pathlib import Path
from typing import Dict, Iterable, List

import yaml

git_root = Path(__file__).absolute().parents[2]


def check_workflows(paths: Iterable[Path]) -> List[str]:
    outputs = []  # type: List[str]
    for path in paths:
        workflow_object = yaml.safe_load(path.read_bytes())
        workflow_object["file---name"] = path.name
        outputs.extend(check_name_override(workflow_object))

    return outputs


def check_name_override(workflow_object: dict) -> List[str]:
    outputs = []  # type: List[str]
    workflow_file = workflow_object.get("file---name", "")  # type: str
    jobs = workflow_object.get("jobs", {})  # type: Dict[str, dict]
    for name, obj in jobs.items():
        header = f"Workflow '{workflow_file}': Job '{name}': "
        name_overriden = obj.get("name", "")
        env_name_overriden = obj.get("env", {}).get("GITHUB_JOB_OVERRIDDEN", "")
        if name_overriden or env_name_overriden:
            if not (name_overriden and env_name_overriden):
                outputs.append(
                    f"{header}job has one of 'name' and 'env.GITHUB_JOB_OVERRIDDEN', "
                    "but not both"
                )
            elif name_overriden != env_name_overriden:
                outputs.append(
                    f"{header}value of 'name' and 'env.GITHUB_JOB_OVERRIDDEN' are not "
                    f"equal. name={name_overriden}; "
                    f"env.GITHUB_JOB_OVERRIDDEN={env_name_overriden}"
                )
    return outputs


def main() -> None:
    reusable_workflow_paths = git_root.glob(".github/workflows/reusable_*.y*ml")
    outputs = check_workflows(reusable_workflow_paths)
    if outputs:
        print("Found next issues for workflows:")
        for o in outputs:
            print(o)


if __name__ == "__main__":
    main()
