#!/bin/bash
set -e
cd /mnt/ch/ClickHouse/.claude/worktrees/agent-ae370eac1a5eda5bc
git log blessed/backport/26.3/109768 --first-parent --oneline -- src/Processors/QueryPlan/QueryPlanFormat.cpp | head -5
echo "---"
git log blessed/backport/26.3/109768 --first-parent --format=%H | while read c; do
  if git cat-file -e "$c:src/Processors/QueryPlan/QueryPlanFormat.cpp" 2>/dev/null; then
    echo "$c PRESENT"
    break
  fi
done
