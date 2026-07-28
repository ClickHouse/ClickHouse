#!/bin/bash
set -e
cd /mnt/ch/ClickHouse/.claude/worktrees/agent-ae370eac1a5eda5bc
while read -r path; do
  git checkout blessed/backport/26.3/109768 -- "$path"
done < <(awk '{print $NF}' /tmp/submodule_diff.txt)
echo "done"
