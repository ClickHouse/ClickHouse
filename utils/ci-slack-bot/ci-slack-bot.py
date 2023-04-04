#!/usr/bin/env python3

# A trivial stateless slack bot that notifies about new broken tests in ClickHouse CI.
# It checks what happened to our CI during the last check_period hours (1 hour) and notifies us in slack if necessary.
# This script should be executed once each check_period hours (1 hour).
# It will post duplicate messages if you run it more often; it will lose some messages if you run it less often.
#
# You can run it locally with no arguments, it will work in a dry-run mode. Or you can set your own SLACK_URL_DEFAULT.
# Feel free to add more checks, more details to messages, or better heuristics.
# NOTE There's no deployment automation for now,
# an AWS Lambda (slack-ci-bot-test lambda in CI-CD) has to be updated manually after changing this script.
#
# See also: https://aretestsgreenyet.com/

import os
import json
import base64

if os.environ.get("AWS_LAMBDA_ENV", "0") == "1":
    # For AWS labmda (python 3.7)
    from botocore.vendored import requests
else:
    # For running locally
    import requests

DRY_RUN_MARK = "<no url, dry run>"

MAX_FAILURES_DEFAULT = 50
SLACK_URL_DEFAULT = DRY_RUN_MARK

# Find tests that failed in master during the last check_period hours,
# but did not fail during the last 2 weeks. Assuming these tests were broken recently.
# NOTE: It may report flaky tests that fail too rarely.
NEW_BROKEN_TESTS_QUERY = """
WITH
    1 AS check_period,
    now() as now
SELECT test_name, any(report_url)
FROM checks
WHERE 1
    AND check_start_time >= now - INTERVAL 1 WEEK
    AND (check_start_time + check_duration_ms / 1000) >= now - INTERVAL check_period HOUR
    AND pull_request_number = 0
    AND test_status LIKE 'F%'
    AND check_status != 'success'
    AND test_name NOT IN (
        SELECT test_name FROM checks WHERE 1
        AND check_start_time >= now - INTERVAL 1 MONTH
        AND (check_start_time + check_duration_ms / 1000) BETWEEN now - INTERVAL 2 WEEK AND now - INTERVAL check_period HOUR 
        AND pull_request_number = 0
        AND check_status != 'success'
        AND test_status LIKE 'F%')
    AND test_context_raw NOT LIKE '%CannotSendRequest%' and test_context_raw NOT LIKE '%Server does not respond to health check%'
GROUP BY test_name
"""

# Returns total number of failed checks during the last 24 hours
# and previous value of that metric (check_period hours ago)
COUNT_FAILURES_QUERY = """
WITH
    1 AS check_period,
    '%' AS check_name_pattern,
    now() as now
SELECT
    countIf((check_start_time + check_duration_ms / 1000) >= now - INTERVAL 24 HOUR) AS new_val,
    countIf((check_start_time + check_duration_ms / 1000) <= now - INTERVAL check_period HOUR) AS prev_val
FROM checks
WHERE 1
    AND check_start_time >= now - INTERVAL 1 WEEK
    AND (check_start_time + check_duration_ms / 1000) >= now - INTERVAL 24 + check_period HOUR
    AND pull_request_number = 0
    AND test_status LIKE 'F%'
    AND check_status != 'success'
    AND check_name ILIKE check_name_pattern
"""

SLACK_MESSAGE_JSON = {"type": "mrkdwn", "text": None}


def get_play_url(query):
    return (
        "https://play.clickhouse.com/play?user=play#"
        + base64.b64encode(query.encode()).decode()
    )


def run_clickhouse_query(query):
    url = "https://play.clickhouse.com/?user=play&query=" + requests.utils.quote(query)
    res = requests.get(url)
    if res.status_code != 200:
        print("Failed to execute query: ", res.status_code, res.content)
        raise Exception(
            "Failed to execute query: {}: {}".format(res.status_code, res.content)
        )

    lines = res.text.strip().splitlines()
    return [x.split("\t") for x in lines]


def get_new_broken_tests_message(broken_tests):
    if not broken_tests:
        return None
    msg = "There are {} new broken tests in master:\n".format(len(broken_tests))
    for name, report in broken_tests:
        msg += " - *{}*  -  <{}|Report>\n".format(name, report)
    return msg


def get_too_many_failures_message(failures_count):
    MAX_FAILURES = int(os.environ.get("MAX_FAILURES", MAX_FAILURES_DEFAULT))
    curr_failures = int(failures_count[0][0])
    prev_failures = int(failures_count[0][1])
    if curr_failures == 0:
        return (
            "Looks like CI is completely broken: there are *no failures* at all... 0_o"
        )
    if curr_failures < MAX_FAILURES:
        return None
    if prev_failures < MAX_FAILURES:
        return "*CI is broken: there are {} failures during the last 24 hours*".format(
            curr_failures
        )
    if curr_failures < prev_failures:
        return None
    if (curr_failures - prev_failures) / prev_failures < 0.2:
        return None
    return "CI is broken and it's getting worse: there are {} failures during the last 24 hours".format(
        curr_failures
    )


def send_to_slack(message):
    SLACK_URL = os.environ.get("SLACK_URL", SLACK_URL_DEFAULT)
    if SLACK_URL == DRY_RUN_MARK:
        return

    payload = SLACK_MESSAGE_JSON.copy()
    payload["text"] = message
    res = requests.post(SLACK_URL, json.dumps(payload))
    if res.status_code != 200:
        print("Failed to send a message to Slack: ", res.status_code, res.content)
        raise Exception(
            "Failed to send a message to Slack: {}: {}".format(
                res.status_code, res.content
            )
        )


def query_and_alert_if_needed(query, get_message_func):
    query_res = run_clickhouse_query(query)
    print("Got result {} for query {}", query_res, query)
    msg = get_message_func(query_res)
    if msg is None:
        return

    msg += "\nCI DB query: <{}|link>".format(get_play_url(query))
    print("Sending message to slack:", msg)
    send_to_slack(msg)


def check_and_alert():
    query_and_alert_if_needed(NEW_BROKEN_TESTS_QUERY, get_new_broken_tests_message)
    query_and_alert_if_needed(COUNT_FAILURES_QUERY, get_too_many_failures_message)


def lambda_handler(event, context):
    try:
        check_and_alert()
        return {"statusCode": 200, "body": "OK"}
    except Exception as e:
        send_to_slack(
            "I failed, please help me (see ClickHouse/utils/ci-slack-bot/ci-slack-bot.py): "
            + str(e)
        )
        return {"statusCode": 200, "body": "FAIL"}


if __name__ == "__main__":
    check_and_alert()
