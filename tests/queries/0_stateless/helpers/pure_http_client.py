import os  
import io                                                                                                                                                                                                         
import sys       
import requests
import time
import pandas as pd

CLICKHOUSE_HOST = os.environ.get('CLICKHOUSE_HOST', '127.0.0.1')
CLICKHOUSE_PORT_HTTP = os.environ.get('CLICKHOUSE_PORT_HTTP', '8123')
CLICKHOUSE_SERVER_URL_STR = 'http://' + ':'.join(str(s) for s in [CLICKHOUSE_HOST, CLICKHOUSE_PORT_HTTP]) + "/"

class ClickHouseClient:
    def __init__(self, host = CLICKHOUSE_SERVER_URL_STR):
        self.host = host

    def query(self, query, connection_timeout = 1500):
        NUMBER_OF_TRIES = 30
        DELAY = 10

        for i in range(NUMBER_OF_TRIES):
            r = requests.post(
                self.host, 
                params = {'timeout_before_checking_execution_speed': 120, 'max_execution_time': 6000},
                timeout = connection_timeout,
                data = query)
            if r.status_code == 200:
                return r.text
            else:
                print('ATTENTION: try #%d failed' % i)
                if i != (NUMBER_OF_TRIES-1):
                    print(query)
                    print(r.text)
                    time.sleep(DELAY*(i+1))
                else:
                    raise ValueError(r.text)

    def query_return_df(self, query, connection_timeout = 1500):
        data = self.query(query, connection_timeout) 
        df = pd.read_csv(io.StringIO(data), sep = '\t')
        return df

    def query_with_data(self, query, content):
        content = content.encode('utf-8')
        r = requests.post(self.host, data=content)
        result = r.text
        if r.status_code == 200:
            return result
        else:
            raise ValueError(r.text)