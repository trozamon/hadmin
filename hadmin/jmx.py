"""
Apache Hadoop-specific JMX parsing
"""

import json
import re

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

    def getVolumesFailed(self):
        return self['.*FSDatasetState-null$']['NumFailedVolumes']
