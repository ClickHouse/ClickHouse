#!/bin/bash
set -e
cd /mnt/ch/ClickHouse/.claude/worktrees/agent-ae370eac1a5eda5bc
commits=$(git rev-list 7c0ebe6329c6631f28ac9a4bcb7d5961850199b1..blessed/backport/26.3/109768 --reverse)
prev_present=1
for c in $commits; do
  if git cat-file -e "$c:src/Processors/QueryPlan/QueryPlanFormat.cpp" 2>/dev/null; then
    present=1
  else
    present=0
  fi
  if [ "$present" != "$prev_present" ]; then
    echo "TRANSITION at $c: now $present"
  fi
  prev_present=$present
done
