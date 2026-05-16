#!/usr/bin/env bash
set -euo pipefail

REPO="ClickHouse/ClickHouse"
AUTHOR="$(gh api user --jq '.login')"

SHARDS=1
SHARD=0
# Number of consecutive PR runs that produce no assistant turn before the
# script gives up. This almost always means a model-side problem such as a
# usage limit, where only the small title-generation submodel still runs.
MAX_EMPTY_STREAK=3

# PRs authored by anyone in this list are skipped entirely - we don't run
# `/continue-pr` on them regardless of which of the three involvement
# filters (authored / assigned / taken-over) would otherwise match.
SKIP_AUTHORS=(alesapin)

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

# Cleans up the per-round temp directory.
cleanup_round_tmp() {
    if [[ -n "${ROUND_TMP:-}" && -d "$ROUND_TMP" ]]; then
        rm -rf "$ROUND_TMP"
    fi
}
trap cleanup_round_tmp EXIT

# Bail out promptly on Ctrl+C / SIGTERM. Without this, when `claude` handles
# SIGINT itself and exits cleanly, the outer `|| ...` clause swallows the
# error and bash advances to the next PR - making the script feel unresponsive
# to Ctrl+C.
on_interrupt() {
    echo ""
    echo "${S}Interrupted, exiting.${R}" >&2
    exit 130
}
trap on_interrupt INT TERM

ROUND=0
EMPTY_STREAK=0
while true; do
    ROUND=$((ROUND + 1))
    echo "${S}##########################################${R}"
    echo "${S}# Round ${ROUND}${R}"
    echo "${S}##########################################${R}"
    echo ""

    cleanup_round_tmp
    ROUND_TMP=$(mktemp -d)

    FIVE_DAYS_AGO=$(date -u -d "5 days ago" +"%Y-%m-%dT%H:%M:%SZ")
    ONE_MONTH_AGO=$(date -u -d "1 month ago" +"%Y-%m-%dT%H:%M:%SZ")

    # Fetch open PRs that involve me (authored, assigned, mentioned, commented,
    # or reviewed by me). This is the universe for all three filters. PRs in
    # which I only pushed a commit without any comment will not appear here -
    # an accepted tradeoff that keeps the candidate set ~hundreds instead of
    # ~thousands.
    echo "${S}Fetching open PRs in ${REPO} involving ${AUTHOR}...${R}"
    INVOLVES_RAW="$ROUND_TMP/involves_raw.json"
    INVOLVES_FILE="$ROUND_TMP/involves.json"
    # `gh search prs` does not expose `baseRefName`, so identify backports by
    # the auto-applied `pr-cherrypick` / `pr-backport` labels (the bot's own
    # markers on PRs that ARE backports). Do not confuse with
    # `pr-must-backport` / `ready-for-backport`, which mark the *original*
    # master PR and would be wrong to skip.
    gh search prs --repo "$REPO" --state open --involves "$AUTHOR" --limit 1000 \
        --json number,title,author,assignees,updatedAt,labels > "$INVOLVES_RAW"
    RAW_COUNT=$(jq 'length' "$INVOLVES_RAW")

    AFTER_BACKPORTS="$ROUND_TMP/after_backports.json"
    jq '[.[] | select((.labels // []) | map(.name) | any(. == "pr-cherrypick" or . == "pr-backport") | not)]' \
        "$INVOLVES_RAW" > "$AFTER_BACKPORTS"
    AFTER_BACKPORTS_COUNT=$(jq 'length' "$AFTER_BACKPORTS")
    SKIPPED_BACKPORTS=$((RAW_COUNT - AFTER_BACKPORTS_COUNT))

    # Drop PRs whose author is on the manual skip list. Authors who reject
    # this kind of help, or whose PRs need handling we shouldn't automate.
    # NB: `.author.login as $a` binds first so that `index($a)` is evaluated
    # against `$skip`, not against the PR object.
    SKIP_AUTHORS_JSON=$(printf '%s\n' "${SKIP_AUTHORS[@]}" | jq -R . | jq -s .)
    jq --argjson skip "$SKIP_AUTHORS_JSON" \
        '[.[] | select(.author.login as $a | ($skip | index($a)) | not)]' \
        "$AFTER_BACKPORTS" > "$INVOLVES_FILE"
    INVOLVES_COUNT=$(jq 'length' "$INVOLVES_FILE")
    SKIPPED_AUTHORS=$((AFTER_BACKPORTS_COUNT - INVOLVES_COUNT))
    echo "${S}Found ${INVOLVES_COUNT} PR(s) involving ${AUTHOR} (skipped ${SKIPPED_BACKPORTS} backport PR(s), ${SKIPPED_AUTHORS} PR(s) by skip-listed authors: ${SKIP_AUTHORS[*]}).${R}"

    # Filter 1: PRs authored by me.
    AUTHORED_FILE="$ROUND_TMP/authored.json"
    jq --arg me "$AUTHOR" '[.[] | select(.author.login == $me)]' \
        "$INVOLVES_FILE" > "$AUTHORED_FILE"
    AUTHORED_COUNT=$(jq 'length' "$AUTHORED_FILE")

    # Filter 2 candidates: I'm an assignee but not the author. These still
    # need the 5-day check (latest commit by the PR's original author must
    # be more than 5 days ago before we touch the PR).
    ASSIGNED_CAND_FILE="$ROUND_TMP/assigned_candidates.json"
    jq --arg me "$AUTHOR" \
        '[.[] | select(.author.login != $me) | select((.assignees // []) | map(.login) | index($me))]' \
        "$INVOLVES_FILE" > "$ASSIGNED_CAND_FILE"
    ASSIGNED_CAND_COUNT=$(jq 'length' "$ASSIGNED_CAND_FILE")

    # Filter 3 candidates: not authored by me, not assigned to me. We will
    # check that I added at least one commit and the latest commit from
    # anyone else was more than one month ago.
    TAKEN_OVER_CAND_FILE="$ROUND_TMP/taken_over_candidates.json"
    jq --arg me "$AUTHOR" \
        '[.[] | select(.author.login != $me) | select((.assignees // []) | map(.login) | index($me) | not)]' \
        "$INVOLVES_FILE" > "$TAKEN_OVER_CAND_FILE"
    TAKEN_OVER_CAND_COUNT=$(jq 'length' "$TAKEN_OVER_CAND_FILE")

    echo "${S}Candidates: ${AUTHORED_COUNT} authored, ${ASSIGNED_CAND_COUNT} assigned (pending 5-day check), ${TAKEN_OVER_CAND_COUNT} potentially taken-over (pending commit check).${R}"

    # Helper: fetch all commits of a PR as a single JSON array on stdout.
    # Falls back to "[]" on failure.
    fetch_commits() {
        local num="$1"
        gh api "repos/${REPO}/pulls/${num}/commits?per_page=100" --paginate 2>/dev/null \
            | jq -s 'add // []' || echo "[]"
    }

    # Resolve assigned candidates via the 5-day check.
    ASSIGNED_NUMS=()
    if (( ASSIGNED_CAND_COUNT > 0 )); then
        echo "${S}Checking last author-commit date for ${ASSIGNED_CAND_COUNT} assigned PR(s)...${R}"
        while IFS= read -r LINE; do
            NUM=$(jq -r '.number' <<< "$LINE")
            PR_AUTHOR=$(jq -r '.author.login' <<< "$LINE")
            [[ -z "$NUM" || "$NUM" == "null" ]] && continue
            COMMITS=$(fetch_commits "$NUM")
            LATEST_AUTHOR_COMMIT=$(jq -r --arg a "$PR_AUTHOR" \
                '[.[] | select((.author.login // "") == $a) | .commit.committer.date] | (max // "")' \
                <<< "$COMMITS")
            if [[ -z "$LATEST_AUTHOR_COMMIT" || "$LATEST_AUTHOR_COMMIT" < "$FIVE_DAYS_AGO" ]]; then
                ASSIGNED_NUMS+=("$NUM")
            fi
        done < <(jq -c '.[]' "$ASSIGNED_CAND_FILE")
    fi

    # Resolve taken-over candidates: must contain at least one commit by me
    # and the latest non-me commit must be older than one month.
    TAKEN_OVER_NUMS=()
    if (( TAKEN_OVER_CAND_COUNT > 0 )); then
        echo "${S}Checking commits for ${TAKEN_OVER_CAND_COUNT} taken-over candidate PR(s)...${R}"
        while IFS= read -r LINE; do
            NUM=$(jq -r '.number' <<< "$LINE")
            [[ -z "$NUM" || "$NUM" == "null" ]] && continue
            COMMITS=$(fetch_commits "$NUM")
            HAS_MINE=$(jq --arg me "$AUTHOR" 'any(.[]; (.author.login // "") == $me)' <<< "$COMMITS")
            [[ "$HAS_MINE" != "true" ]] && continue
            LATEST_OTHER=$(jq -r --arg me "$AUTHOR" \
                '[.[] | select((.author.login // "") != $me) | .commit.committer.date] | (max // "")' \
                <<< "$COMMITS")
            if [[ -n "$LATEST_OTHER" && "$LATEST_OTHER" < "$ONE_MONTH_AGO" ]]; then
                TAKEN_OVER_NUMS+=("$NUM")
            fi
        done < <(jq -c '.[]' "$TAKEN_OVER_CAND_FILE")
    fi

    # Build the merged list as JSON: authored + selected assigned + selected
    # taken-over, deduped by number, sorted by updatedAt ascending.
    SELECTED_NUMS_FILE="$ROUND_TMP/selected_nums.json"
    {
        jq -r '.[].number' "$AUTHORED_FILE"
        printf '%s\n' "${ASSIGNED_NUMS[@]:-}"
        printf '%s\n' "${TAKEN_OVER_NUMS[@]:-}"
    } | awk 'NF' | jq -R 'tonumber' | jq -s 'unique' > "$SELECTED_NUMS_FILE"

    MERGED_FILE="$ROUND_TMP/merged.json"
    jq --slurpfile nums "$SELECTED_NUMS_FILE" \
        '[.[] | select(.number as $n | $nums[0] | index($n))] | sort_by(.updatedAt)' \
        "$INVOLVES_FILE" > "$MERGED_FILE"

    PRS=$(jq -r '.[] | "\(.number)\t\(.title)"' "$MERGED_FILE")

    echo "${S}Selected: ${AUTHORED_COUNT} authored, ${#ASSIGNED_NUMS[@]} assigned, ${#TAKEN_OVER_NUMS[@]} taken-over.${R}"

    if [[ -z "$PRS" ]]; then
        echo "${S}No matching open PRs found. Sleeping 60s before retrying...${R}"
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

        # Switch back to master before each claude invocation. The inner
        # `/continue-pr` checks out the PR branch, and after it's done its
        # work and pushed, we need to leave the cwd on a known branch so
        # that the next iteration's `claude --print` finds the project-level
        # `continue-pr` skill (which lives under `.claude/skills/` and so
        # is only present on branches that carry it - notably *not* on
        # backport branches like `backport/26.2/...`). `-f` discards any
        # uncommitted state left behind by the previous PR's work; by this
        # point everything that should persist has already been pushed.
        if ! git checkout -f master 2>/dev/null; then
            echo "${S}WARNING: git checkout master failed before PR #${NUMBER}; skipping${R}"
            continue
        fi

        # Tee the raw stream-json output to a file so we can both render it
        # live and, after the run, check whether the main model actually took
        # a turn. Without that check, model-side errors (e.g. usage limits)
        # are silent: only the title-generation submodel runs, no `assistant`
        # event is emitted, and the `result` event - which carries the error
        # subtype - was dropped by the jq filter below.
        RAW="$ROUND_TMP/pr_${NUMBER}.jsonl"
        : > "$RAW"
        # `timeout --foreground` keeps `claude` in the same process group as
        # bash, so the terminal driver delivers Ctrl+C / `SIGINT` to `claude`
        # too. Without `--foreground`, `timeout` puts the child in a fresh
        # process group, `SIGINT` only reaches bash + the rest of the
        # pipeline, the pipeline keeps waiting on `claude`, and bash can
        # never get to the `on_interrupt` trap.
        timeout --foreground 3600 claude --dangerously-skip-permissions --print --verbose \
            --output-format stream-json \
            "/continue-pr https://github.com/${REPO}/pull/${NUMBER}" \
            < /dev/null 2>&1 \
            | tee "$RAW" \
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
                elif .type == "result" then
                    if (.is_error // false) or ((.subtype // "") | startswith("error")) then
                        "\n!!! claude error (\(.subtype // "result")): \(((.result // .message // .error // .) | tostring)[0:800])\n"
                    else empty end
                else empty end' \
            || echo "${S}WARNING: claude pipeline exited with code $? for PR #${NUMBER}, continuing...${R}"

        # An "assistant" event is emitted for every main-model turn. If none
        # was produced, the model did not actually work on this PR. A handful
        # of these in a row almost always means a usage limit or other
        # API-level failure - keep going just burns through the PR list.
        if grep -q '"type":"assistant"' "$RAW" 2>/dev/null; then
            EMPTY_STREAK=0
        else
            EMPTY_STREAK=$((EMPTY_STREAK + 1))
            echo "${S}NOTE: claude produced no assistant turns for PR #${NUMBER} (${EMPTY_STREAK}/${MAX_EMPTY_STREAK} consecutive empty runs).${R}"
            if (( EMPTY_STREAK >= MAX_EMPTY_STREAK )); then
                # ROUND_TMP is deleted by the EXIT trap, so copy the raw
                # stream out before bailing so it can be inspected.
                KEEP=$(mktemp -t "continue-all-prs-empty-pr${NUMBER}.XXXXXX.jsonl")
                cp "$RAW" "$KEEP" 2>/dev/null || KEEP="$RAW"
                echo "${S}Aborting: ${MAX_EMPTY_STREAK} PRs in a row produced no work. Most likely a usage limit was hit. Last raw stream is at ${KEEP}.${R}" >&2
                exit 1
            fi
        fi

        echo ""
        echo "${S}Done with PR #${NUMBER}${R}"
        echo ""
    done <<< "$SHARD_PRS"

    echo "${S}Round ${ROUND} complete. Starting over...${R}"
    echo ""
done
