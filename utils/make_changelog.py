#!/usr/bin/env python3
# Note: should work with python 2 and 3


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
                except Exception:
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
    except Exception:
        logging.error('Cannot find merge base for %s and %s', first, second)
        raise

def rev_parse(rev, project_root):
    try:
        command = "git rev-parse {}".format(rev)
        text = subprocess.check_output(command, shell=True, cwd=project_root)
        text = text.decode('utf-8', 'ignore')
        sha = tuple(filter(len, text.split()))[0]
        check_sha(sha)
        return sha
    except Exception:
        logging.error('Cannot find revision %s', rev)
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


# Get list of commits a specified commit is cherry-picked from. Can return an empty list.
def parse_original_commits_from_cherry_pick_message(commit_message):
    prefix = '(cherry picked from commits'
    pos = commit_message.find(prefix)
    if pos == -1:
        prefix = '(cherry picked from commit'
        pos = commit_message.find(prefix)
        if pos == -1:
            return []
    pos += len(prefix)
    endpos = commit_message.find(')', pos)
    if endpos == -1:
        return []
    lst = [x.strip() for x in commit_message[pos:endpos].split(',')]
    lst = [x for x in lst if x]
    return lst


# Use GitHub search api to check if commit from any pull request. Update pull_requests info.
def find_pull_request_for_commit(commit_info, pull_requests, token, max_retries, retry_timeout):
    commits = [commit_info['sha']] + parse_original_commits_from_cherry_pick_message(commit_info['commit']['message'])

    # Special case for cherry-picked merge commits without -x option. Parse pr number from commit message and search it.
    if commit_info['commit']['message'].startswith('Merge pull request'):
        tokens = commit_info['commit']['message'][len('Merge pull request'):].split()
        if len(tokens) > 0 and tokens[0].startswith('#'):
            pr_number = tokens[0][1:]
            if len(pr_number) > 0 and pr_number.isdigit():
                commits = [pr_number]

    query = 'search/issues?q={}+type:pr+repo:{}&sort=created&order=asc'.format(' '.join(commits), repo)
    resp = github_api_get_json(query, token, max_retries, retry_timeout)

    found = False
    for item in resp['items']:
        if 'pull_request' in item:
            found = True
            number = item['number']
            if number not in pull_requests:
                pull_requests[number] = {
                    'title': item['title'],
                    'description': item['body'],
                    'user': item['user']['login'],
                }

    return found


# Find pull requests from list of commits. If no pull request found, add commit to not_found_commits list.
def find_pull_requests(commits, commits_info, token, max_retries, retry_timeout):
    not_found_commits = []
    pull_requests = {}

    for i, commit in enumerate(commits):
        if (i + 1) % 10 == 0:
            logging.info('Processed %d commits', i + 1)
        if not find_pull_request_for_commit(commits_info[commit], pull_requests, token, max_retries, retry_timeout):
            not_found_commits.append(commit)

    return not_found_commits, pull_requests


# Find pull requests by list of numbers
def find_pull_requests_by_num(pull_requests_nums, token, max_retries, retry_timeout):
    pull_requests = {}

    for pr in pull_requests_nums:
        item = github_api_get_json('repos/{}/pulls/{}'.format(repo, pr), token, max_retries, retry_timeout)

        number = item['number']
        if number not in pull_requests:
            pull_requests[number] = {
                'title': item['title'],
                'description': item['body'],
                'user': item['user']['login'],
            }

    return pull_requests


# Get users for all unknown commits and pull requests.
def get_users_info(pull_requests, commits_info, token, max_retries, retry_timeout):

    users = {}

    def update_user(user):
        if user not in users:
            query = 'users/{}'.format(user)
            resp = github_api_get_json(query, token, max_retries, retry_timeout)
            users[user] = resp

    for pull_request in list(pull_requests.values()):
        update_user(pull_request['user'])

    for commit_info in list(commits_info.values()):
        if 'committer' in commit_info and commit_info['committer'] is not None and 'login' in commit_info['committer']:
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

        name = None
        login = None
        author = None

        if not info['author']:
            author = 'Unknown'
        else:
            # GitHub login
            if 'login' in info['author']:
                login = info['author']['login']

                # First, try get name from github user
                try:
                    name = users[login]['name']
                except KeyError:
                    pass
            else:
                login = 'Unknown'

            # Then, try get name from commit
            if not name:
                try:
                    name = info['commit']['author']['name']
                except KeyError:
                    pass

            author = '[{}]({})'.format(name or login, info['author']['html_url'])

        texts.append(pattern.format(commit, html_url, author, msg))

    text = 'Commits which are not from any pull request:\n\n'
    return text + '\n\n'.join(texts)

# This function mirrors the PR description checks in ClickhousePullRequestTrigger.
# Returns False if the PR should not be mentioned changelog.
def parse_one_pull_request(item):
    description = item['description']
    lines = [line for line in [x.strip() for x in description.split('\n')] if line] if description else []
    lines = [re.sub(r'\s+', ' ', l) for l in lines]

    cat_pos = None
    short_descr_pos = None
    long_descr_pos = None

    if lines:
        for i in range(len(lines) - 1):
            if re.match(r'(?i).*category.*:$', lines[i]):
                cat_pos = i
            if re.match(r'(?i)^\**\s*(Short description|Change\s*log entry)', lines[i]):
                short_descr_pos = i
            if re.match(r'(?i)^\**\s*Detailed description', lines[i]):
                long_descr_pos = i

    if cat_pos is None:
        return False
    cat = lines[cat_pos + 1]
    cat = re.sub(r'^[-*\s]*', '', cat)

    # Filter out the PR categories that are not for changelog.
    if re.match(r'(?i)doc|((non|in|not|un)[-\s]*significant)|(not[ ]*for[ ]*changelog)', cat):
        return False

    short_descr = ''
    if short_descr_pos:
        short_descr_end = long_descr_pos or len(lines)
        short_descr = lines[short_descr_pos + 1]
        if short_descr_pos + 2 != short_descr_end:
            short_descr += ' ...'

    # If we have nothing meaningful
    if not re.match('\w', short_descr):
        short_descr = item['title']

    # TODO: Add detailed description somewhere

    item['entry'] = short_descr
    item['category'] = cat

    return True


# List of pull requests -> text description.
def process_pull_requests(pull_requests, users, repo):
    groups = {}

    for id, item in list(pull_requests.items()):
        if not parse_one_pull_request(item):
            continue

        pattern = "{} [#{}]({}) ({})"
        link = 'https://github.com/{}/pull/{}'.format(repo, id)
        author = 'author not found'
        if item['user'] in users:
            # TODO get user from any commit if no user name on github
            user = users[item['user']]
            author = '[{}]({})'.format(user['name'] or user['login'], user['html_url'])

        cat = item['category']
        if cat not in groups:
            groups[cat] = []
        groups[cat].append(pattern.format(item['entry'], id, link, author))

    categories_preferred_order = ['Backward Incompatible Change', 'New Feature', 'Bug Fix', 'Improvement', 'Performance Improvement', 'Build/Testing/Packaging Improvement', 'Other']

    def categories_sort_key(name):
        if name in categories_preferred_order:
            return str(categories_preferred_order.index(name)).zfill(3)
        else:
            return name.lower()

    texts = []
    for group, text in sorted(list(groups.items()), key = lambda kv: categories_sort_key(kv[0])):
        items = ['* {}'.format(pr) for pr in text]
        texts.append('### {}\n{}'.format(group if group else '[No category]', '\n'.join(items)))

    return '\n\n'.join(texts)


# Load inner state. For debug purposes.
def load_state(state_file, base_sha, new_tag, prev_tag):

    state = {}

    if state_file:
        try:
            if os.path.exists(state_file):
                logging.info('Reading state from %s', state_file)
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


def make_changelog(new_tag, prev_tag, pull_requests_nums, repo, repo_folder, state_file, token, max_retries, retry_timeout):

    base_sha = None
    if new_tag and prev_tag:
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

    if base_sha:
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
            unknown_commits, pull_requests = find_pull_requests(commits, commits_info, token, max_retries, retry_timeout)
            state['unknown_commits'] = unknown_commits
            state['pull_requests'] = pull_requests
    else:
        pull_requests = find_pull_requests_by_num(pull_requests_nums.split(','), token, max_retries, retry_timeout)

    logging.info('Found %d pull requests and %d unknown commits.\n', len(pull_requests), len(unknown_commits))
    save_state(state_file, state)

    if not is_users_loaded:
        logging.info('Getting users info using github api.')
        users = get_users_info(pull_requests, commits_info, token, max_retries, retry_timeout)
        state['users'] = users

    logging.info('Found %d users.', len(users))
    save_state(state_file, state)

    changelog = '{}\n\n{}'.format(process_pull_requests(pull_requests, users, repo), process_unknown_commits(unknown_commits, commits_info, users))

    # Substitute links to issues
    changelog = re.sub(r'(?<!\[)#(\d{4,})(?!\])', r'[#\1](https://github.com/{}/issues/\1)'.format(repo), changelog)

    # Remove double whitespaces and trailing whitespaces
    changelog = re.sub(r' {2,}| +$', r''.format(repo), changelog)

    print(changelog.encode('utf-8'))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Make changelog.')
    parser.add_argument('prev_release_tag', nargs='?', help='Git tag from previous release.')
    parser.add_argument('new_release_tag', nargs='?', help='Git tag for new release.')
    parser.add_argument('--pull-requests', help='Process the specified list of pull-request numbers (comma separated) instead of commits between tags.')
    parser.add_argument('--token', help='Github token. Use it to increase github api query limit.')
    parser.add_argument('--directory', help='ClickHouse repo directory. Script dir by default.')
    parser.add_argument('--state', help='File to dump inner states result.', default='changelog_state.json')
    parser.add_argument('--repo', help='ClickHouse repo on GitHub.', default='ClickHouse/ClickHouse')
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
    pull_requests = args.pull_requests

    if (not prev_release_tag or not new_release_tag) and not pull_requests:
        raise Exception('Either release tags or --pull-requests must be specified')

    if prev_release_tag and new_release_tag and pull_requests:
        raise Exception('Either release tags or --pull-requests must be specified')

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

    repo_folder = os.path.expanduser(repo_folder)
    new_release_tag = rev_parse(new_release_tag, repo_folder)
    prev_release_tag = rev_parse(prev_release_tag, repo_folder)

    make_changelog(new_release_tag, prev_release_tag, pull_requests, repo, repo_folder, state_file, token, max_retry, retry_timeout)
