# -*- coding: utf-8 -*-

import requests
import time


class Query:
    '''Implements queries to the Github API using GraphQL
    '''

    def __init__(self, token, max_page_size=100, min_page_size=5):
        self._token = token
        self._max_page_size = max_page_size
        self._min_page_size = min_page_size
        self.api_costs = {}

    _MEMBERS = '''
        organization(login: "{organization}") {{
            team(slug: "{team}") {{
                members(first: {max_page_size} {next}) {{
                    pageInfo {{
                        hasNextPage
                        endCursor
                    }}
                    nodes {{
                        login
                    }}
                }}
            }}
        }}
    '''
    def get_members(self, organization, team):
        '''Get all team members for organization

        Returns:
            logins: a list of members' logins
        '''
        logins = []
        not_end = True
        query = Query._MEMBERS.format(organization=organization,
                                      team=team,
                                      max_page_size=self._max_page_size,
                                      next='')

        while not_end:
            result = self._run(query)['organization']['team']
            if result is None:
                break
            result = result['members']
            not_end = result['pageInfo']['hasNextPage']
            query = Query._MEMBERS.format(organization=organization,
                                          team=team,
                                          max_page_size=self._max_page_size,
                                          next=f'after: "{result["pageInfo"]["endCursor"]}"')

            logins += [node['login'] for node in result['nodes']]

        return logins

    _LABELS = '''
        repository(owner: "ClickHouse" name: "ClickHouse") {{
            pullRequest(number: {number}) {{
                labels(first: {max_page_size} {next}) {{
                    pageInfo {{
                        hasNextPage
                        endCursor
                    }}
                    nodes {{
                        name
                        color
                    }}
                }}
            }}
        }}
    '''
    def get_labels(self, pull_request):
        '''Fetchs all labels for given pull-request

        Args:
            pull_request: JSON object returned by `get_pull_requests()`

        Returns:
            labels: a list of JSON nodes with the name and color fields
        '''
        labels = [label for label in pull_request['labels']['nodes']]
        not_end = pull_request['labels']['pageInfo']['hasNextPage']
        query = Query._LABELS.format(number = pull_request['number'],
                                     max_page_size = self._max_page_size,
                                     next=f'after: "{pull_request["labels"]["pageInfo"]["endCursor"]}"')

        while not_end:
            result = self._run(query)['repository']['pullRequest']['labels']
            not_end = result['pageInfo']['hasNextPage']
            query = Query._LABELS.format(number=pull_request['number'],
                                         max_page_size=self._max_page_size,
                                         next=f'after: "{result["pageInfo"]["endCursor"]}"')

            labels += [label for label in result['nodes']]

        return labels

    _TIMELINE = '''
        repository(owner: "ClickHouse" name: "ClickHouse") {{
            pullRequest(number: {number}) {{
                timeline(first: {max_page_size} {next}) {{
                    pageInfo {{
                        hasNextPage
                        endCursor
                    }}
                    nodes {{
                        ... on CrossReferencedEvent {{
                            isCrossRepository
                            source {{
                                ... on PullRequest {{
                                    number
                                    baseRefName
                                    merged
                                    labels(first: {max_page_size}) {{
                                        pageInfo {{
                                            hasNextPage
                                            endCursor
                                        }}
                                        nodes {{
                                            name
                                            color
                                        }}
                                    }}
                                }}
                            }}
                            target {{
                                ... on PullRequest {{
                                    number
                                }}
                            }}
                        }}
                    }}
                }}
            }}
        }}
    '''
    def get_timeline(self, pull_request):
        '''Fetchs all cross-reference events from pull-request's timeline

        Args:
            pull_request: JSON object returned by `get_pull_requests()`

        Returns:
            events: a list of JSON nodes for CrossReferenceEvent
        '''
        events = [event for event in pull_request['timeline']['nodes'] if event and event['source']]
        not_end = pull_request['timeline']['pageInfo']['hasNextPage']
        query = Query._TIMELINE.format(number = pull_request['number'],
                                       max_page_size = self._max_page_size,
                                       next=f'after: "{pull_request["timeline"]["pageInfo"]["endCursor"]}"')

        while not_end:
            result = self._run(query)['repository']['pullRequest']['timeline']
            not_end = result['pageInfo']['hasNextPage']
            query = Query._TIMELINE.format(number=pull_request['number'],
                                           max_page_size=self._max_page_size,
                                           next=f'after: "{result["pageInfo"]["endCursor"]}"')

            events += [event for event in result['nodes'] if event and event['source']]

        return events

    _PULL_REQUESTS = '''
        repository(owner: "ClickHouse" name: "ClickHouse") {{
            defaultBranchRef {{
                name
                target {{
                    ... on Commit {{
                        history(first: {max_page_size} {next}) {{
                            pageInfo {{
                                hasNextPage
                                endCursor
                            }}
                            nodes {{
                                oid
                                associatedPullRequests(first: {min_page_size}) {{
                                    totalCount
                                    nodes {{
                                        ... on PullRequest {{
                                            id
                                            number
                                            author {{
                                                login
                                            }}
                                            bodyText
                                            mergedBy {{
                                                login
                                            }}
                                            url
                                            baseRefName
                                            baseRepository {{
                                                nameWithOwner
                                            }}
                                            mergeCommit {{
                                                oid
                                            }}
                                            labels(first: {min_page_size}) {{
                                                pageInfo {{
                                                    hasNextPage
                                                    endCursor
                                                }}
                                                nodes {{
                                                    name
                                                    color
                                                }}
                                            }}
                                            timeline(first: {min_page_size}) {{
                                                pageInfo {{
                                                    hasNextPage
                                                    endCursor
                                                }}
                                                nodes {{
                                                    ... on CrossReferencedEvent {{
                                                        isCrossRepository
                                                        source {{
                                                            ... on PullRequest {{
                                                                number
                                                                baseRefName
                                                                merged
                                                                labels(first: 0) {{
                                                                    nodes {{
                                                                        name
                                                                    }}
                                                                }}
                                                            }}
                                                        }}
                                                        target {{
                                                            ... on PullRequest {{
                                                                number
                                                            }}
                                                        }}
                                                    }}
                                                }}
                                            }}
                                        }}
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}
            }}
        }}
    '''
    def get_pull_requests(self, before_commit, login):
        '''Get all merged pull-requests from the HEAD of default branch to the last commit (excluding)

        Args:
            before_commit (string-convertable): commit sha of the last commit (excluding)
            login (string): filter pull-requests by user login

        Returns:
            pull_requests: a list of JSON nodes with pull-requests' details
        '''
        pull_requests = []
        not_end = True
        query = Query._PULL_REQUESTS.format(max_page_size=self._max_page_size,
                                            min_page_size=self._min_page_size,
                                            next='')

        while not_end:
            result = self._run(query)['repository']['defaultBranchRef']
            default_branch_name = result['name']
            result = result['target']['history']
            not_end = result['pageInfo']['hasNextPage']
            query = Query._PULL_REQUESTS.format(max_page_size=self._max_page_size,
                                                min_page_size=self._min_page_size,
                                                next=f'after: "{result["pageInfo"]["endCursor"]}"')

            for commit in result['nodes']:
                if str(commit['oid']) == str(before_commit):
                    not_end = False
                    break

                # TODO: fetch all pull-requests that were merged in a single commit.
                assert commit['associatedPullRequests']['totalCount'] <= self._min_page_size, \
                    f'there are {commit["associatedPullRequests"]["totalCount"]} pull-requests merged in commit {commit["oid"]}'

                for pull_request in commit['associatedPullRequests']['nodes']:
                    if(pull_request['baseRepository']['nameWithOwner'] == 'ClickHouse/ClickHouse' and
                       pull_request['baseRefName'] == default_branch_name and
                       pull_request['mergeCommit']['oid'] == commit['oid'] and
                       (not login or pull_request['author']['login'] == login)):
                        pull_requests.append(pull_request)

        return pull_requests

    _DEFAULT = '''
        repository(owner: "ClickHouse", name: "ClickHouse") {
            defaultBranchRef {
                name
            }
        }
    '''
    def get_default_branch(self):
        '''Get short name of the default branch

        Returns:
            name (string): branch name
        '''
        return self._run(Query._DEFAULT)['repository']['defaultBranchRef']['name']

    _GET_LABEL = '''
        repository(owner: "ClickHouse" name: "ClickHouse") {{
            labels(first: {max_page_size} {next} query: "{name}") {{
                pageInfo {{
                    hasNextPage
                    endCursor
                }}
                nodes {{
                    id
                    name
                    color
                }}
            }}
        }}
    '''
    _SET_LABEL = '''
        addLabelsToLabelable(input: {{ labelableId: "{pr_id}", labelIds: "{label_id}" }}) {{
            clientMutationId
        }}
    '''
    def set_label(self, pull_request, label_name):
        '''Set label by name to the pull request

        Args:
            pull_request: JSON object returned by `get_pull_requests()`
            label_name (string): label name
        '''
        labels = []
        not_end = True
        query = Query._GET_LABEL.format(name=label_name,
                                        max_page_size=self._max_page_size,
                                        next='')

        while not_end:
            result = self._run(query)['repository']['labels']
            not_end = result['pageInfo']['hasNextPage']
            query = Query._GET_LABEL.format(name=label_name,
                                            max_page_size=self._max_page_size,
                                            next=f'after: "{result["pageInfo"]["endCursor"]}"')

            labels += [label for label in result['nodes']]

        if not labels:
            return

        query = Query._SET_LABEL.format(pr_id = pull_request['id'], label_id = labels[0]['id'])
        self._run(query, is_mutation=True)

        pull_request['labels']['nodes'].append(labels[0])


    def _run(self, query, is_mutation=False):
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        def requests_retry_session(
            retries=3,
            backoff_factor=0.3,
            status_forcelist=(500, 502, 504),
            session=None,
        ):
            session = session or requests.Session()
            retry = Retry(
                total=retries,
                read=retries,
                connect=retries,
                backoff_factor=backoff_factor,
                status_forcelist=status_forcelist,
            )
            adapter = HTTPAdapter(max_retries=retry)
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            return session

        headers = {'Authorization': f'bearer {self._token}'}
        if is_mutation:
            query = f'''
            mutation {{
                {query}
            }}
            '''
        else:
            query = f'''
            query {{
                {query}
                rateLimit {{
                    cost
                    remaining
                }}
            }}
            '''

        while True:
            request = requests_retry_session().post('https://api.github.com/graphql', json={'query': query}, headers=headers)
            if request.status_code == 200:
                result = request.json()
                if 'errors' in result:
                    raise Exception(f'Errors occured: {result["errors"]}')

                if not is_mutation:
                    import inspect
                    caller = inspect.getouterframes(inspect.currentframe(), 2)[1][3]
                    if caller not in self.api_costs.keys():
                        self.api_costs[caller] = 0
                    self.api_costs[caller] += result['data']['rateLimit']['cost']

                return result['data']
            else:
                import json
                resp = request.json()
                if resp and len(resp) > 0 and resp[0] and 'type' in resp[0] and resp[0]['type'] == 'RATE_LIMITED':
                    print("API rate limit exceeded. Waiting for 1 second.")
                    time.sleep(1)
                    continue

                raise Exception(f'Query failed with code {request.status_code}:\n{json.dumps(resp, indent=4)}')
