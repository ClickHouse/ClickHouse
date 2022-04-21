# -*- coding: utf-8 -*-
import json
import requests
import time
import os

DB = 'gh-data'
RETRIES = 5

API_URL = 'https://api.github.com/repos/ClickHouse/ClickHouse/'


def _reverse_dict_with_list(source):
    result = {}
    for key, value in list(source.items()):
        for elem in value:
            result[elem] = key
    return result


MARKER_TO_LABEL = {
    '- New Feature': ['pr-feature'],
    '- Bug Fix': ['pr-bugfix'],
    '- Improvement': ['pr-improvement'],
    '- Performance Improvement': ['pr-performance'],
    '- Backward Incompatible Change': ['pr-backward-incompatible'],
    '- Build/Testing/Packaging Improvement': ['pr-build'],
    '- Documentation': ['pr-documentation', 'pr-doc-fix'],
    '- Other': ['pr-other'],
    '- Not for changelog': ['pr-not-for-changelog']
}

LABEL_TO_MARKER = _reverse_dict_with_list(MARKER_TO_LABEL)

DOC_ALERT_LABELS = {
    'pr-feature'
}


def set_labels_for_pr(pull_request_number, labels, headers):
    data = {
        "labels": list(labels)
    }

    for i in range(RETRIES):
        try:
            response = requests.put(API_URL + 'issues/' + str(pull_request_number) + '/labels', json=data, headers=headers)
            response.raise_for_status()
            break
        except Exception as ex:
            print(("Exception", ex))
            time.sleep(0.2)


def get_required_labels_from_desc(description, current_labels):
    result = set([])
    # find first matching category
    for marker, labels in list(MARKER_TO_LABEL.items()):
        if marker in description:
            if not any(label in current_labels for label in labels):
                result.add(labels[0])
            break

    # if no category than leave as is
    if not result:
        return current_labels

    # save all old labels except category label
    for label in current_labels:
        if label not in result and label not in LABEL_TO_MARKER:
            result.add(label)

    # if some of labels require doc alert
    if any(label in result for label in DOC_ALERT_LABELS):
        result.add('doc-alert')

    return result


def label_pull_request_event(response):
    pull_request = response['pull_request']
    current_labels = set([label['name'] for label in pull_request['labels']])
    pr_description = pull_request['body'] if pull_request['body'] else ''
    required_labels = get_required_labels_from_desc(pr_description, current_labels)
    if not required_labels.issubset(current_labels):
        token = os.getenv('GITHUB_TOKEN')
        auth = {'Authorization': 'token ' + token}
        set_labels_for_pr(pull_request['number'], required_labels, auth)


def process_issue_event(response):
    issue = response['issue']
    return dict(
        action=response['action'],
        sender=response['sender']['login'],
        updated_at=issue['updated_at'],
        url=issue['url'],
        number=issue['number'],
        author=issue['user']['login'],
        labels=[label['name'] for label in issue['labels']],
        state=issue['state'],
        assignees=[assignee['login'] for assignee in issue['assignees']],
        created_at=issue['created_at'],
        body=issue['body'] if issue['body'] else '',
        title=issue['title'],
        comments=issue['comments'],
        raw_json=json.dumps(response),)


def process_issue_comment_event(response):
    issue = response['issue']
    comment = response['comment']

    return dict(
        action='comment_' + response['action'],
        sender=response['sender']['login'],
        updated_at=issue['updated_at'],
        url=issue['url'],
        number=issue['number'],
        author=issue['user']['login'],
        labels=[label['name'] for label in issue['labels']],
        state=issue['state'],
        assignees=[assignee['login'] for assignee in issue['assignees']],
        created_at=issue['created_at'],
        body=issue['body'] if issue['body'] else '',
        title=issue['title'],
        comments=issue['comments'],
        comment_body=comment['body'],
        comment_author=comment['user']['login'],
        comment_url=comment['url'],
        comment_created_at=comment['created_at'],
        comment_updated_at=comment['updated_at'],
        raw_json=json.dumps(response),)


def process_pull_request_event(response):
    pull_request = response['pull_request']
    result = dict(
        updated_at=pull_request['updated_at'],
        number=pull_request['number'],
        action=response['action'],
        sender=response['sender']['login'],
        url=pull_request['url'],
        author=pull_request['user']['login'],
        labels=[label['name'] for label in pull_request['labels']],
        state=pull_request['state'],
        body=pull_request['body'] if pull_request['body'] else '',
        title=pull_request['title'],
        created_at=pull_request['created_at'],
        assignees=[assignee['login'] for assignee in pull_request['assignees']],
        requested_reviewers=[reviewer['login'] for reviewer in pull_request['requested_reviewers']],
        head_repo=pull_request['head']['repo']['full_name'],
        head_ref=pull_request['head']['ref'],
        head_clone_url=pull_request['head']['repo']['clone_url'],
        head_ssh_url=pull_request['head']['repo']['ssh_url'],
        base_repo=pull_request['base']['repo']['full_name'],
        base_ref=pull_request['base']['ref'],
        base_clone_url=pull_request['base']['repo']['clone_url'],
        base_ssh_url=pull_request['base']['repo']['ssh_url'],
        raw_json=json.dumps(response),
    )

    if 'mergeable' in pull_request and pull_request['mergeable'] is not None:
        result['mergeable'] = 1 if pull_request['mergeable'] else 0

    if 'merged_by' in pull_request and pull_request['merged_by'] is not None:
        result['merged_by'] = pull_request['merged_by']['login']

    if 'merged_at' in pull_request and pull_request['merged_at'] is not None:
        result['merged_at'] = pull_request['merged_at']

    if 'closed_at' in pull_request and pull_request['closed_at'] is not None:
        result['closed_at'] = pull_request['closed_at']

    if 'merge_commit_sha' in pull_request and pull_request['merge_commit_sha'] is not None:
        result['merge_commit_sha'] = pull_request['merge_commit_sha']

    if 'draft' in pull_request:
        result['draft'] = 1 if pull_request['draft'] else 0

    for field in ['comments', 'review_comments', 'commits', 'additions', 'deletions', 'changed_files']:
        if field in pull_request:
            result[field] = pull_request[field]

    return result


def process_pull_request_review(response):
    result = process_pull_request_event(response)
    review = response['review']
    result['action'] = 'review_' + result['action']
    result['review_body'] = review['body'] if review['body'] is not None else ''
    result['review_id'] = review['id']
    result['review_author'] = review['user']['login']
    result['review_commit_sha'] = review['commit_id']
    result['review_submitted_at'] = review['submitted_at']
    result['review_state'] = review['state']
    return result


def process_pull_request_review_comment(response):
    result = process_pull_request_event(response)
    comment = response['comment']
    result['action'] = 'review_comment_' + result['action']
    result['review_id'] = comment['pull_request_review_id']
    result['review_comment_path'] = comment['path']
    result['review_commit_sha'] = comment['commit_id']
    result['review_comment_body'] = comment['body']
    result['review_comment_author'] = comment['user']['login']
    result['review_comment_created_at'] = comment['created_at']
    result['review_comment_updated_at'] = comment['updated_at']
    return result


def process_push(response):
    common_part = dict(
        before_sha=response['before'],
        after_sha=response['after'],
        full_ref=response['ref'],
        ref=response['ref'].split('/')[-1],
        repo=response['repository']['full_name'],
        pusher=response['pusher']['name'],
        sender=response['sender']['login'],
        pushed_at=response['repository']['pushed_at'],
        raw_json=json.dumps(response),
    )
    commits = response['commits']
    result = []
    for commit in commits:
        commit_dict = common_part.copy()
        commit_dict['sha'] = commit['id']
        commit_dict['tree_sha'] = commit['tree_id']
        commit_dict['author'] = commit['author']['name']
        commit_dict['committer'] = commit['committer']['name']
        commit_dict['message'] = commit['message']
        commit_dict['commited_at'] = commit['timestamp']
        result.append(commit_dict)
    return result


def event_processor_dispatcher(headers, body, inserter):
    if 'X-Github-Event' in headers:
        if headers['X-Github-Event'] == 'issues':
            result = process_issue_event(body)
            inserter.insert_event_into(DB, 'issues', result)
        elif headers['X-Github-Event'] == 'issue_comment':
            result = process_issue_comment_event(body)
            inserter.insert_event_into(DB, 'issues', result)
        elif headers['X-Github-Event'] == 'pull_request':
            result = process_pull_request_event(body)
            inserter.insert_event_into(DB, 'pull_requests', result)
            label_pull_request_event(body)
        elif headers['X-Github-Event'] == 'pull_request_review':
            result = process_pull_request_review(body)
            inserter.insert_event_into(DB, 'pull_requests', result)
        elif headers['X-Github-Event'] == 'pull_request_review_comment':
            result = process_pull_request_review_comment(body)
            inserter.insert_event_into(DB, 'pull_requests', result)
        elif headers['X-Github-Event'] == 'push':
            result = process_push(body)
            inserter.insert_events_into(DB, 'commits', result)


class ClickHouseInserter(object):
    def __init__(self, url, user, password):
        self.url = url
        self.auth = {
            'X-ClickHouse-User': user,
            'X-ClickHouse-Key': password
        }

    def _insert_json_str_info(self, db, table, json_str):
        params = {
            'database': db,
            'query': 'INSERT INTO {table} FORMAT JSONEachRow'.format(table=table),
            'date_time_input_format': 'best_effort'
        }
        for i in range(RETRIES):
            response = None
            try:
                response = requests.post(self.url, params=params, data=json_str, headers=self.auth, verify=False)
                response.raise_for_status()
                break
            except Exception as ex:
                print(("Cannot insert with exception %s", str(ex)))
                if response:
                    print(("Response text %s", response.text))
                time.sleep(0.1)
        else:
            raise Exception("Cannot insert data into clickhouse")

    def insert_event_into(self, db, table, event):
        event_str = json.dumps(event)
        self._insert_json_str_info(db, table, event_str)

    def insert_events_into(self, db, table, events):
        jsons = []
        for event in events:
            jsons.append(json.dumps(event))

        self._insert_json_str_info(db, table, ','.join(jsons))


def test(event, context):
    inserter = ClickHouseInserter(
        os.getenv('CLICKHOUSE_URL'),
        os.getenv('CLICKHOUSE_USER'),
        os.getenv('CLICKHOUSE_PASSWORD'))

    body = json.loads(event['body'], strict=False)
    headers = event['headers']
    event_processor_dispatcher(headers, body, inserter)

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/plain'
        },
        'isBase64Encoded': False,
    }
