""" Hadoop REST api handling """

import json

try:
    from http.client import HTTPConnection
except ImportError:
    from httplib import HTTPConnection


class NodeManagerREST:

    def __init__(self, json_str=''):
        self.load(json_str)

    def load(self, json_str=''):
        try:
            self.data = json.loads(json_str)['nodeInfo']
        except ValueError:
            self.data = dict()
        except KeyError:
            self.data = dict()

    def load_from_host(self, addr):
        conn = HTTPConnection(addr)
        return self.load_from_connection(conn)

    def load_from_connection(self, conn):
        conn.request('GET', '/ws/v1/node')
        res = conn.getresponse()
        if res.status == 200:
            self.load(res.read())

    def isHealthy(self):
        return self.data['nodeHealthy']

    def getHealthReport(self):
        return self.data['healthReport']