"""
Apache Hadoop-specific JMX parsing
"""

import json
import re

try:
    from http.client import HTTPConnection
except ImportError:
    from httplib import HTTPConnection

class JMX(dict):
    """
    Class representing JMX values
    """

    def __init__(self, json_str=''):
        self.load(json_str)


    def load(self, json_str):
        try:
            beans = json.loads(json_str)

            for bean in beans['beans']:
                try:
                    self[bean['name']] = bean
                except KeyError:
                    pass

        except ValueError:
            pass

    def load_from_host(self, addr):
        conn = HTTPConnection(addr)
        return self.load_from_connection(conn)

    def load_from_connection(self, conn):
        conn.request('GET', '/jmx')
        res = conn.getresponse()
        if res.status == 200:
            self.load(res.read())

    def __getitem__(self, k):
        if k in self.keys():
            return self.get(k)
        else:
            for key in self.keys():
                if re.match(k, key) is not None:
                    return self.get(key)

        raise KeyError(k)


class DataNodeJMX(JMX):

    def __init__(self, json_str=''):
        self.load(json_str)

    def getFailedVolumes(self):
        return self['.*FSDatasetState-null$']['NumFailedVolumes']


class ResponseMock:

    def __init__(self, content, status):
        self.content = content
        self.status = status

    def read(self):
        return self.content

class ConnectionMock:

    def request(self, req_type, path):
        if req_type == 'GET' and path == '/jmx':
            self.requested = True

    def getresponse(self):
        if self.requested:
            with open('data/datanode.jmx.json') as f:
                return ResponseMock(f.read(), 200)

        return ResponseMock('', 404)
