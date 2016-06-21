from unittest2 import TestCase
from hadmin.rest import NodeManagerREST
from hadmin import mock


class NodeManagerRESTTest(TestCase):

    def testHealthy(self):
        self.assertEqual(self.rest.isHealthy(), True)

    def testHealthReport(self):
        self.assertEqual(self.rest.getHealthReport(),
                         "1/2 local-dirs are bad: /var/hadoop/compute; ")

    def setUp(self):
        self.rest = NodeManagerREST()

        with open('data/nodemanager.rest.json') as f:
            self.rest = NodeManagerREST(f.read())


class NodeManagerRESTNetworkTest(NodeManagerRESTTest):

    def setUp(self):
        self.rest = NodeManagerREST()
        self.rest.load_from_connection(mock.RESTConnectionMock())
