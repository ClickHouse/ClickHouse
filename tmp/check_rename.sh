#!/bin/bash
cd /mnt/ch/ClickHouse/.claude/worktrees/agent-ae370eac1a5eda5bc
git show 1e0acd6a91a0c82d20bad23f0e19785698289de7:src/Processors/QueryPlan/QueryPlanFormat.cpp | grep -oE '\b[A-Za-z_][A-Za-z0-9_]*\(' | sed 's/($//' | sort -u > /tmp/qpf_funcs.txt
while read -r f; do
  fname="${f%(}"
  n=$(git grep -c "$fname(" blessed/backport/26.3/109768 -- 'src/*' 2>/dev/null | wc -l)
  if [ "$n" -gt 0 ]; then echo "$fname found in $n files"; fi
done < /tmp/qpf_funcs.txt
