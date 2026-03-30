#!/usr/bin/env bash
set -euo pipefail

REPO="ClickHouse/ClickHouse"
AUTHOR="$(gh api user --jq '.login')"

SHARDS=1
SHARD=0

# Bright magenta for script messages, to distinguish from Claude output
S=$'\033[1;35m'
R=$'\033[0m'

while [[ $# -gt 0 ]]; do
    case "$1" in
        --shards) SHARDS="$2"; shift 2 ;;
        --shard)  SHARD="$2";  shift 2 ;;
        *) echo "${S}Unknown option: $1${R}" >&2; exit 1 ;;
    esac
done

if (( SHARD >= SHARDS )); then
    echo "${S}Error: --shard (${SHARD}) must be less than --shards (${SHARDS})${R}" >&2
    exit 1
fi

echo "${S}Shard ${SHARD} of ${SHARDS}${R}"
echo ""

ROUND=0
while true; do
    ROUND=$((ROUND + 1))
    echo "${S}##########################################${R}"
    echo "${S}# Round ${ROUND}${R}"
    echo "${S}##########################################${R}"
    echo ""
    echo "${S}Fetching open PRs by ${AUTHOR} in ${REPO}...${R}"

    PRS=$(gh api "repos/${REPO}/pulls?state=open&per_page=100" --paginate \
        --jq "[.[] | select(.user.login == \"${AUTHOR}\")] | sort_by(.updated_at) | .[] | \"\(.number)\t\(.title)\"")

    if [[ -z "$PRS" ]]; then
        echo "${S}No open PRs found. Sleeping 60s before retrying...${R}"
        sleep 60
        continue
    fi

    # Filter to only PRs matching this shard
    SHARD_PRS=""
    while IFS=$'\t' read -r NUMBER TITLE; do
        if (( NUMBER % SHARDS == SHARD )); then
            if [[ -n "$SHARD_PRS" ]]; then
                SHARD_PRS+=$'\n'
            fi
            SHARD_PRS+="${NUMBER}"$'\t'"${TITLE}"
        fi
    done <<< "$PRS"

    if [[ -z "$SHARD_PRS" ]]; then
        echo "${S}No PRs matching shard ${SHARD}/${SHARDS}. Sleeping 60s before retrying...${R}"
        sleep 60
        continue
    fi

    COUNT=$(echo "$SHARD_PRS" | wc -l)
    echo "${S}Found ${COUNT} open PR(s) for shard ${SHARD}/${SHARDS}:${R}"
    echo "${S}${SHARD_PRS}${R}"
    echo ""

    I=0
    while IFS=$'\t' read -r NUMBER TITLE; do
        I=$((I + 1))
        echo "${S}==========================================${R}"
        echo "${S}[${I}/${COUNT}] PR #${NUMBER}: ${TITLE}${R}"
        echo "${S}==========================================${R}"

        timeout 3600 claude --dangerously-skip-permissions --print --verbose \
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
                else empty end' \
            || echo "${S}WARNING: claude exited with code $? for PR #${NUMBER}, continuing...${R}"

        echo ""
        echo "${S}Done with PR #${NUMBER}${R}"
        echo ""
    done <<< "$SHARD_PRS"

    echo "${S}Round ${ROUND} complete. Starting over...${R}"
    echo ""
done
