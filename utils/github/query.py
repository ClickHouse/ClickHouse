# -*- coding: utf-8 -*-

import requests


class Query:
    '''Implements queries to the Github API using GraphQL
    '''

    def __init__(self, token, max_page_size=100):
        self._token = token
        self._max_page_size = max_page_size

    _LABELS = '''
    {{
        repository(owner: "yandex" name: "ClickHouse") {{
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
        not_end = bool(pull_request['labels']['pageInfo']['hasNextPage'])
        query = Query._LABELS.format(number=pull_request['number'], max_page_size=self._max_page_size, next=f'after: "{pull_request["labels"]["pageInfo"]["endCursor"]}"')

        while not_end:
            result = self._run(query)['data']['repository']['pullRequest']['labels']
            not_end = result['pageInfo']['hasNextPage']

            labels += [label for label in result['nodes']]

            query = Query._LABELS.format(number=pull_request['number'], max_page_size=self._max_page_size, next=f'after: "{result["pageInfo"]["endCursor"]}"')

        return labels

    _PULL_REQUESTS = '''
    {{
        repository(owner: "yandex" name: "ClickHouse") {{
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
                                associatedPullRequests(first: 5) {{
                                    totalCount
                                    nodes {{
                                        ... on PullRequest {{
                                            number
                                            url
                                            baseRefName
                                            baseRepository {{
                                                nameWithOwner
                                            }}
                                            mergeCommit {{
                                                oid
                                                author {{
                                                    name
                                                }}
                                            }}
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
                                }}
                            }}
                        }}
                    }}
                }}
            }}
        }}
    }}
    '''
    def get_pull_requests(self, before_commit):
        '''Get all merged pull-requests from the HEAD of default branch to the last commit (excluding)

        Args:
            before_commit (string-convertable): commit sha of the last commit (excluding)

        Returns:
            pull_requests: a list of JSON nodes with pull-requests' details
        '''
        pull_requests = []
        query = Query._PULL_REQUESTS.format(max_page_size=self._max_page_size, next='')
        not_end = True

        while not_end:
            result = self._run(query)['data']['repository']['defaultBranchRef']
            default_branch_name = result['name']
            result = result['target']['history']
            not_end = result['pageInfo']['hasNextPage']

            for commit in result['nodes']:
                if str(commit['oid']) == str(before_commit):
                    not_end = False
                    break

                # TODO: use helper to fetch all pull-requests that were merged in a single commit.
                assert commit['associatedPullRequests']['totalCount'] <= self._max_page_size, \
                    f'there are {commit["associatedPullRequests"]["totalCount"]} pull-requests merged in commit {commit["oid"]}'

                for pull_request in commit['associatedPullRequests']['nodes']:
                    if(pull_request['baseRepository']['nameWithOwner'] == 'yandex/ClickHouse' and
                       pull_request['baseRefName'] == default_branch_name and
                       pull_request['mergeCommit']['oid'] == commit['oid']):
                        pull_requests.append(pull_request)

            query = Query._PULL_REQUESTS.format(max_page_size=self._max_page_size, next=f'after: "{result["pageInfo"]["endCursor"]}"')

        return pull_requests

    _DEFAULT = '''
    {
        repository(owner: "yandex", name: "ClickHouse") {
            defaultBranchRef {
                name
            }
        }
    }
    '''
    def get_default_branch(self):
        '''Get short name of the default branch

        Returns:
            name (string): branch name
        '''
        return self._run(Query._DEFAULT)['data']['repository']['defaultBranchRef']['name']

    def _run(self, query):
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
        request = requests_retry_session().post('https://api.github.com/graphql', json={'query': query}, headers=headers)
        if request.status_code == 200:
            return request.json()
        else:
            raise Exception(f'Query failed with code {request.status_code}: {query}')
