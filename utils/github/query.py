# -*- coding: utf-8 -*-

import requests
import sys


class Query:
    def __init__(self, token, max_page_size=100, pull_request_page_size=2):
        self._token = token
        self._max_page_size = max_page_size
        self._pull_request_page_size = pull_request_page_size

    ''' Get all pull-requests associated with at least one commit in range (until, default head],
        where "default head" is a head of default repository branch.
    '''
    def get_pull_requests(self, until):
        pull_requests = {} # number → (merge oid, labels)
        query = Query._FIRST.format(max_page_size=self._max_page_size, pull_request_page_size=self._pull_request_page_size)
        not_end = True

        while not_end:
            result = self._run(query)['data']['repository']['defaultBranchRef']['target']['history']
            not_end = result['pageInfo']['hasNextPage']

            for commit in result['edges']:
                node = commit['node']

                if str(node['oid']) == str(until):
                    not_end = False
                    break

                # TODO: fetch all pull-requests that were merged in a single commit.
                assert node['associatedPullRequests']['totalCount'] <= self._pull_request_page_size

                for pull_request in node['associatedPullRequests']['nodes']:
                    if pull_request['mergeCommit']['oid'] == node['oid']:
                        pull_requests[pull_request['number']] = (node['oid'], self._labels(pull_request))

            query = Query._NEXT.format(max_page_size=self._max_page_size,
                                       pull_request_page_size=self._pull_request_page_size, history_cursor=result['pageInfo']['endCursor'])

        return pull_requests

    def get_default_branch(self):
        return self._run(Query._DEFAULT)['data']['repository']['defaultBranchRef']['name']

    def _labels(self, pull_request):
        # TODO: fetch all labels
        return [label['node']['name'] for label in pull_request['labels']['edges']]

    def _run(self, query):
        headers = {'Authorization': f'bearer {self._token}'}
        request = requests.post('https://api.github.com/graphql', json={'query': query}, headers=headers)
        if request.status_code == 200:
            return request.json()
        else:
            raise Exception(f'Query failed with code {request.status_code}: {query}')

# TODO: switch to some query composer.
Query._FIRST = '''
{{
  repository(owner: "yandex", name: "ClickHouse") {{
    defaultBranchRef {{
      target {{
        ... on Commit {{
          history(first: {max_page_size}) {{
            pageInfo {{
              endCursor
              hasNextPage
            }}
            edges {{
              node {{
                oid
                associatedPullRequests(first: {pull_request_page_size}) {{
                  totalCount
                  nodes {{
                    ... on PullRequest {{
                      number
                      mergeCommit {{
                        oid
                      }}
                      labels(first: {max_page_size}) {{
                        pageInfo {{
                          endCursor
                          hasNextPage
                        }}
                        edges {{
                          node {{
                            name
                          }}
                          cursor
                        }}
                      }}
                    }}
                  }}
                }}
              }}
              cursor
            }}
          }}
        }}
      }}
    }}
  }}
}}
'''

Query._NEXT = '''
{{
  repository(owner: "yandex", name: "ClickHouse") {{
    defaultBranchRef {{
      target {{
        ... on Commit {{
          history(first: {max_page_size}, after: "{history_cursor}") {{
            pageInfo {{
              endCursor
              hasNextPage
            }}
            edges {{
              node {{
                oid
                associatedPullRequests(first: {pull_request_page_size}) {{
                  totalCount
                  nodes {{
                    ... on PullRequest {{
                      number
                      mergeCommit {{
                        oid
                      }}
                      labels(first: {max_page_size}) {{
                        pageInfo {{
                          endCursor
                          hasNextPage
                        }}
                        edges {{
                          node {{
                            name
                          }}
                          cursor
                        }}
                      }}
                    }}
                  }}
                }}
              }}
              cursor
            }}
          }}
        }}
      }}
    }}
  }}
}}
'''

Query._DEFAULT = '''
{
  repository(owner: "yandex", name: "ClickHouse") {
    defaultBranchRef {
      name
    }
  }
}
'''
