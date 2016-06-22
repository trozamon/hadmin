from unittest2 import TestCase
from hadmin.rest import NodeManager
from hadmin import mock


class NodeManagerTest(TestCase):

    def testHealthy(self):
        self.assertEqual(self.rest.isHealthy(), True)

    def testHealthReport(self):
        self.assertEqual(self.rest.getHealthReport(),
                         "1/2 local-dirs are bad: /var/hadoop/compute; ")

    def setUp(self):
        self.rest = NodeManager()

        with open('data/nodemanager.rest.json') as f:
            self.rest = NodeManager(f.read())


class NodeManagerNetworkTest(NodeManagerTest):

    def setUp(self):
        self.rest = NodeManager()
        self.rest.load_from_connection(mock.RESTConnectionMock())
