#!/bin/bash
set -e
cd /mnt/ch/ClickHouse/.claude/worktrees/agent-ae370eac1a5eda5bc

# Paths present in backport (status M or D): checkout backport's version
while read -r status path; do
  case "$status" in
    M|D) git checkout blessed/backport/26.3/109768 -- "$path" ;;
  esac
done < <(awk '{print $5, $6}' /tmp/submodule_diff.txt)

# Paths only added in HEAD (status A, absent from backport): remove
while read -r status path; do
  case "$status" in
    A) git rm --cached -r -q "$path" ;;
  esac
done < <(awk '{print $5, $6}' /tmp/submodule_diff.txt)

echo "done"
