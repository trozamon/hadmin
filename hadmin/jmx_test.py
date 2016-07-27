from unittest2 import TestCase
from hadmin import mock
from hadmin.jmx import DataNodeJMX, JMX, NameNodeJMX


class JMXTest(TestCase):

    def testGetRuntime(self):
        self.assertEqual(self.jmx['java.lang:type=Runtime']['SpecVersion'],
                         '1.8')

    def testGetVolumesFailed(self):
        k = 'Hadoop:service=DataNode,name=FSDatasetState-null'
        self.assertEqual(self.jmx[k]['NumFailedVolumes'], 0)

    def testGetVolumesFailedWithRegex(self):
        k = '.*FSDatasetState-null$'
        self.assertEqual(self.jmx[k]['NumFailedVolumes'], 0)

    def testMissingKey(self):
        with self.assertRaises(KeyError):
            self.jmx['blah']

    def setUp(self):
        self.jmx = JMX()

        with open('data/datanode.jmx.json') as f:
            self.jmx.load(f.read())


class DataNodeJMXTest(TestCase):

    def testVolumesFailed(self):
        self.assertEqual(self.jmx.getFailedVolumes(), 0)

    def setUp(self):
        self.jmx = DataNodeJMX()

        with open('data/datanode.jmx.json') as f:
            self.jmx = DataNodeJMX(f.read())


class JMXNetworkTest(JMXTest):

    def setUp(self):
        self.jmx = JMX()

        self.jmx.load_from_connection(mock.JMXConnectionMock())


class NameNodeJMXTest(TestCase):

    def testHeapMemoryUsed(self):
        self.assertEqual(73726608, self.jmx.getHeapMemoryUsed())

    def testNumThreads(self):
        self.assertEqual(34, self.jmx.getNumThreads())

    def testTotalCapacity(self):
        self.assertEqual(2.0, self.jmx.getTotalCapacity())

    def testUsedCapacity(self):
        self.assertEqual(0.0, self.jmx.getUsedCapacity())

    def testBlocksPendingReplication(self):
        self.assertEqual(1, self.jmx.getBlocksPendingReplication())

    def testUnderReplicatedBlocks(self):
        self.assertEqual(2, self.jmx.getUnderReplicatedBlocks())

    def testCorruptBlocks(self):
        self.assertEqual(3, self.jmx.getCorruptBlocks())

    def setUp(self):
        self.jmx = NameNodeJMX()

        with open('data/namenode.jmx.json') as f:
            self.jmx = NameNodeJMX(f.read())
