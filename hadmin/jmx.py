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


class NameNodeJMX(JMX):
    """
    NameNode/HDFS statistics from JMX

    Since the NameNode is responsible for managing HDFS metadata, various
    statistics about the NameNode and HDFS can be obtained from an instance of
    this class.
    """

    def __init__(self, json_str=''):
        self.load(json_str)

    def getHeapMemoryUsed(self):
        """
        Get the amount of memory used of the JVM Heap, in bytes
        """

        return self['^java.lang:type=Memory$']['HeapMemoryUsage']['used']

    def getNumThreads(self):
        """
        Get the number of currently spawned threads the NameNode is running
        """

        return self['^java.lang:type=Threading$']['ThreadCount']

    def getTotalCapacity(self):
        """
        Get the total capacity of HDFS, in GiB
        """

        tmp = self['^Hadoop:service=NameNode,name=FSNamesystem$']
        return tmp['CapacityTotalGB']

    def getUsedCapacity(self):
        """
        Get the used capacity of HDFS, in GiB
        """

        tmp = self['^Hadoop:service=NameNode,name=FSNamesystem$']
        return tmp['CapacityUsedGB']

    def getUnderReplicatedBlocks(self):
        """
        Get the number of under-replicated blocks in HDFS
        """

        tmp = self['^Hadoop:service=NameNode,name=FSNamesystem$']
        return tmp['UnderReplicatedBlocks']

    def getCorruptBlocks(self):
        """
        Get the number of corrupt blocks in HDFS
        """

        tmp = self['^Hadoop:service=NameNode,name=FSNamesystem$']
        return tmp['CorruptBlocks']

    def getBlocksPendingReplication(self):
        """
        Get the number of blocks whose replication is currently pending
        """

        tmp = self['^Hadoop:service=NameNode,name=FSNamesystem$']
        return tmp['PendingReplicationBlocks']
