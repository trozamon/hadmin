"""
Apache Hadoop-specific JMX parsing
----------------------------------

Parse JMX JSON objects to get some stats
"""

import json
import re

try:
    from http.client import HTTPConnection
except ImportError:
    from httplib import HTTPConnection


class JMX(dict):
    """
    Base class that does the majority of the JMX/JSON work.

    Subclass this in order to provide nice, easy-to-use wrappers.
    """

    def __init__(self, json_str=''):
        self.load(json_str)

    def load(self, json_str):
        """
        Load JMX data from a JSON string
        """

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
        """
        Load JMX data from a host
        """

        conn = HTTPConnection(addr)
        return self.load_from_connection(conn)

    def load_from_connection(self, conn):
        """
        Load JMX data from a connection. Connections must have a
        :py:func:`request` function and a
        :py:func:`getresponse` function.
        """

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
