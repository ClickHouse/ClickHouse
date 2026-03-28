#!/usr/bin/env bash
set -euo pipefail

REPO="ClickHouse/ClickHouse"
AUTHOR="$(gh api user --jq '.login')"

SHARDS=1
SHARD=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --shards) SHARDS="$2"; shift 2 ;;
        --shard)  SHARD="$2";  shift 2 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

if (( SHARD >= SHARDS )); then
    echo "Error: --shard (${SHARD}) must be less than --shards (${SHARDS})" >&2
    exit 1
fi

ROUND=0
while true; do
    ROUND=$((ROUND + 1))
    echo "##########################################"
    echo "# Round ${ROUND}"
    echo "##########################################"
    echo ""
    echo "Fetching open PRs by ${AUTHOR} in ${REPO}..."

    PRS=$(gh api "repos/${REPO}/pulls?state=open&per_page=100" --paginate \
        --jq "[.[] | select(.user.login == \"${AUTHOR}\")] | sort_by(.updated_at) | .[] | \"\(.number)\t\(.title)\"")

    if [[ -z "$PRS" ]]; then
        echo "No open PRs found. Sleeping 60s before retrying..."
        sleep 60
        continue
    fi

    COUNT=$(echo "$PRS" | wc -l)
    echo "Found ${COUNT} open PR(s):"
    echo "$PRS"
    echo ""

    I=0
    while IFS=$'\t' read -r NUMBER TITLE; do
        if (( NUMBER % SHARDS != SHARD )); then
            continue
        fi
        I=$((I + 1))
        echo "=========================================="
        echo "[${I}/${COUNT}] PR #${NUMBER}: ${TITLE}"
        echo "=========================================="

        claude --dangerously-skip-permissions --print --verbose \
            --output-format stream-json \
            "/continue-pr https://github.com/${REPO}/pull/${NUMBER}" \
            < /dev/null 2>&1 \
            | jq --unbuffered -r '
                if .type == "assistant" then
                    (.message.content[] |
                        if .type == "text" then .text
                        elif .type == "tool_use" then "\n>>> \(.name): \(.input | tostring | .[0:500])\n"
                        else empty end)
                elif .type == "user" then
                    (.message.content[] |
                        if .type == "tool_result" then "<<< \(.content | tostring | .[0:300])\n"
                        else empty end)
                else empty end'

        echo ""
        echo "Done with PR #${NUMBER}"
        echo ""
    done <<< "$PRS"

    echo "Round ${ROUND} complete. Starting over..."
    echo ""
done
