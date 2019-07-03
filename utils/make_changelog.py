#!/usr/bin/env python
# Note: should work with python 2 and 3
from __future__ import print_function

import requests
import json
import subprocess
import re
import os
import time
import logging
import codecs
import argparse


GITHUB_API_URL = 'https://api.github.com/'
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def http_get_json(url, token, max_retries, retry_timeout):

    for t in range(max_retries):

        if token:
            resp = requests.get(url, headers={"Authorization": "token {}".format(token)})
        else:
            resp = requests.get(url)

        if resp.status_code != 200:
            msg = "Request {} failed with code {}.\n{}\n".format(url, resp.status_code, resp.text)

            if resp.status_code == 403 or resp.status_code >= 500:
                try:
                    if (resp.json()['message'].startswith('API rate limit exceeded') or resp.status_code >= 500) and t + 1 < max_retries:
                        logging.warning(msg)
                        time.sleep(retry_timeout)
                        continue
                except:
                    pass

            raise Exception(msg)

        return resp.json()


def github_api_get_json(query, token, max_retries, retry_timeout):
    return http_get_json(GITHUB_API_URL + query, token, max_retries, retry_timeout)


def check_sha(sha):
    if not (re.match('^[a-hA-H0-9]+$', sha) and len(sha) >= 7):
        raise Exception("String " + sha + " doesn't look like git sha.")


def get_merge_base(first, second, project_root):
    try:
        command = "git merge-base {} {}".format(first, second)
        text = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, cwd=project_root).stdout.read()
        text = text.decode('utf-8', 'ignore')
        sha = tuple(filter(len, text.split()))[0]
        check_sha(sha)
        return sha
    except:
        logging.error('Cannot find merge base for %s and %s', first, second)
        raise


# Get list of commits from branch to base_sha. Update commits_info.
def get_commits_from_branch(repo, branch, base_sha, commits_info, max_pages, token, max_retries, retry_timeout):

    def get_commits_from_page(page):
        query = 'repos/{}/commits?sha={}&page={}'.format(repo, branch, page)
        resp = github_api_get_json(query, token, max_retries, retry_timeout)
        for commit in resp:
            sha = commit['sha']
            if sha not in commits_info:
                commits_info[sha] = commit

        return [commit['sha'] for commit in resp]

    commits = []
    found_base_commit = False

    for page in range(max_pages):
        page_commits = get_commits_from_page(page)
        for commit in page_commits:
            if commit == base_sha:
                found_base_commit = True
                break
            commits.append(commit)

        if found_base_commit:
            break

    if not found_base_commit:
        raise Exception("Can't found base commit sha {} in branch {}. Checked {} commits on {} pages.\nCommits: {}"
                        .format(base_sha, branch, len(commits), max_pages, ' '.join(commits)))
    return commits


# Use GitHub search api to check if commit from any pull request. Update pull_requests info.
def find_pull_request_for_commit(commit_sha, pull_requests, token, max_retries, retry_timeout):
    resp = github_api_get_json('search/issues?q={}'.format(commit_sha), token, max_retries, retry_timeout)

    found = False
    for item in resp['items']:
        if 'pull_request' in item:
            found = True
            number = item['number']
            if number not in pull_requests:
                pull_requests[number] = {
                    'description': item['body'],
                    'user': item['user']['login'],
                }

    return found


# Find pull requests from list of commits. If no pull request found, add commit to not_found_commits list.
def find_pull_requests(commits, token, max_retries, retry_timeout):
    not_found_commits = []
    pull_requests = {}

    for i, commit in enumerate(commits):
        if (i + 1) % 10 == 0:
            logging.info('Processed %d commits', i + 1)
        if not find_pull_request_for_commit(commit, pull_requests, token, max_retries, retry_timeout):
            not_found_commits.append(commit)

    return not_found_commits, pull_requests


# Get users for all unknown commits and pull requests.
def get_users_info(pull_requests, commits_info, token, max_retries, retry_timeout):

    users = {}

    def update_user(user):
        if user not in users:
            query = 'users/{}'.format(user)
            resp = github_api_get_json(query, token, max_retries, retry_timeout)
            users[user] = resp

    for pull_request in pull_requests.values():
        update_user(pull_request['user'])

    for commit_info in commits_info.values():
        if 'author' in commit_info and commit_info['author'] is not None:
            update_user(commit_info['committer']['login'])
        else:
            logging.warning('Not found author for commit %s.', commit_info['html_url'])

    return users


# List of unknown commits -> text description.
def process_unknown_commits(commits, commits_info, users):

    pattern = 'Commit: [{}]({})\nAuthor: {}\nMessage: {}'

    texts = []

    for commit in commits:
        info = commits_info[commit]
        html_url = info['html_url']
        msg = info['commit']['message']

        # GitHub login
        login = info['author']['login']
        name = None

        # Firstly, try get name from github user
        try:
            name = users[login]['name']
        except:
            pass

        # Then, try get name from commit
        if not name:
            try:
                name = info['commit']['author']['name']
            except:
                pass

        author = '[{}]({})'.format(name or login, info['author']['html_url'])
        texts.append(pattern.format(commit, html_url, author, msg))

    text = 'Commits which are not from any pull request:\n\n'
    return text + '\n\n'.join(texts)


# List of pull requests -> text description.
def process_pull_requests(pull_requests, users, repo):
    groups = {}

    for id, item in pull_requests.items():
        lines = list(filter(len, map(lambda x: x.strip(), item['description'].split('\n'))))

        cat_pos = None
        short_descr_pos = None
        long_descr_pos = None

        if lines:
            for i in range(len(lines) - 1):
                if re.match('^\**Category\s*(\(leave one\))*:*\**\s*$', lines[i]):
                    cat_pos = i
                if re.match('^\**\s*Short description', lines[i]):
                    short_descr_pos = i
                if re.match('^\**\s*Detailed description', lines[i]):
                    long_descr_pos = i

        cat = ''
        if cat_pos:
            # TODO: Sometimes have more than one
            cat = lines[cat_pos + 1]
        cat = cat.strip().lstrip('-').strip()

        short_descr = ''
        if short_descr_pos:
            short_descr_end = long_descr_pos or len(lines)
            short_descr = lines[short_descr_pos + 1]
            if short_descr_pos + 2 != short_descr_end:
                short_descr += ' ...'

        # TODO: Add detailed description somewhere

        pattern = u"{} [#{}]({}) ({})"
        link = 'https://github.com/{}/pull/{}'.format(repo, id)
        author = 'author not found'
        if item['user'] in users:
            # TODO get user from any commit if no user name on github
            user = users[item['user']]
            author = u'[{}]({})'.format(user['name'] or user['login'], user['html_url'])

        if cat not in groups:
            groups[cat] = []
        groups[cat].append(pattern.format(short_descr, id, link, author))

    texts = []
    for group, text in groups.items():
        items = [u'* {}'.format(pr) for pr in text]
        texts.append(u'### {}\n{}'.format(group if group else u'[No category]', '\n'.join(items)))

    return '\n\n'.join(texts)


# Load inner state. For debug purposes.
def load_state(state_file, base_sha, new_tag, prev_tag):

    state = {}

    if state_file:
        try:
            if os.path.exists(state_file):
                logging.info('Reading state from %', state_file)
                with codecs.open(state_file, encoding='utf-8') as f:
                    state = json.loads(f.read())
            else:
                logging.info('State file does not exist. Will create new one.')
        except Exception as e:
            logging.warning('Cannot load state from %s. Reason: %s', state_file, str(e))

    if state:
        if 'base_sha' not in state or 'new_tag' not in state or 'prev_tag' not in state:
            logging.warning('Invalid state. Will create new one.')
        elif state['base_sha'] == base_sha and state['new_tag'] == new_tag and state['prev_tag'] == prev_tag:
            logging.info('State loaded.')
        else:
            logging.info('Loaded state has different tags or merge base sha. Will create new state.')
            state = {}

    return state


# Save inner state. For debug purposes.
def save_state(state_file, state):
    with codecs.open(state_file, 'w', encoding='utf-8') as f:
        f.write(json.dumps(state, indent=4, separators=(',', ': ')))


def make_changelog(new_tag, prev_tag, repo, repo_folder, state_file, token, max_retries, retry_timeout):

    base_sha = get_merge_base(new_tag, prev_tag, repo_folder)
    logging.info('Base sha: %s', base_sha)

    # Step 1. Get commits from merge_base to new_tag HEAD.
    # Result is a list of commits + map with commits info (author, message)
    commits_info = {}
    commits = []
    is_commits_loaded = False

    # Step 2. For each commit check if it is from any pull request (using github search api).
    # Result is a list of unknown commits + map with pull request info (author, description).
    unknown_commits = []
    pull_requests = {}
    is_pull_requests_loaded = False

    # Step 3. Map users with their info (Name)
    users = {}
    is_users_loaded = False

    # Step 4. Make changelog text from data above.

    state = load_state(state_file, base_sha, new_tag, prev_tag)

    if state:

        if 'commits' in state and 'commits_info' in state:
            logging.info('Loading commits from %s', state_file)
            commits_info = state['commits_info']
            commits = state['commits']
            is_commits_loaded = True

        if 'pull_requests' in state and 'unknown_commits' in state:
            logging.info('Loading pull requests from %s', state_file)
            unknown_commits = state['unknown_commits']
            pull_requests = state['pull_requests']
            is_pull_requests_loaded = True

        if 'users' in state:
            logging.info('Loading users requests from %s', state_file)
            users = state['users']
            is_users_loaded = True

    state['base_sha'] = base_sha
    state['new_tag'] = new_tag
    state['prev_tag'] = prev_tag

    if not is_commits_loaded:
        logging.info('Getting commits using github api.')
        commits = get_commits_from_branch(repo, new_tag, base_sha, commits_info, 100, token, max_retries, retry_timeout)
        state['commits'] = commits
        state['commits_info'] = commits_info

    logging.info('Found %d commits from %s to %s.\n', len(commits), new_tag, base_sha)
    save_state(state_file, state)

    if not is_pull_requests_loaded:
        logging.info('Searching for pull requests using github api.')
        unknown_commits, pull_requests = find_pull_requests(commits, token, max_retries, retry_timeout)
        state['unknown_commits'] = unknown_commits
        state['pull_requests'] = pull_requests

    logging.info('Found %d pull requests and %d unknown commits.\n', len(pull_requests), len(unknown_commits))
    save_state(state_file, state)

    if not is_users_loaded:
        logging.info('Getting users info using github api.')
        users = get_users_info(pull_requests, commits_info, token, max_retries, retry_timeout)
        state['users'] = users

    logging.info('Found %d users.', len(users))
    save_state(state_file, state)

    print(process_pull_requests(pull_requests, users, repo))
    print(process_unknown_commits(unknown_commits, commits_info, users))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Make changelog.')
    parser.add_argument('prev_release_tag', help='Git tag from previous release.')
    parser.add_argument('new_release_tag', help='Git tag for new release.')
    parser.add_argument('--token', help='Github token. Use it to increase github api query limit. ')
    parser.add_argument('--directory', help='ClickHouse repo directory. Script dir by default.')
    parser.add_argument('--state', help='File to dump inner states result.', default='changelog_state.json')
    parser.add_argument('--repo', help='ClickHouse repo on GitHub.', default='yandex/ClickHouse')
    parser.add_argument('--max_retry', default=100, type=int,
                        help='Max number of retries pre api query in case of API rate limit exceeded error.')
    parser.add_argument('--retry_timeout', help='Timeout after retry in seconds.', type=int, default=5)

    args = parser.parse_args()
    prev_release_tag = args.prev_release_tag
    new_release_tag = args.new_release_tag
    token = args.token or ''
    repo_folder = args.directory or SCRIPT_DIR
    state_file = args.state
    repo = args.repo
    max_retry = args.max_retry
    retry_timeout = args.retry_timeout

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

    repo_folder = os.path.expanduser(repo_folder)

    make_changelog(new_release_tag, prev_release_tag, repo, repo_folder, state_file, token, max_retry, retry_timeout)
