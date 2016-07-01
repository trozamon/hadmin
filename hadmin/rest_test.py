from unittest2 import TestCase
from hadmin.rest import NodeManager, ResourceManager
from hadmin.rest import NM_INFO_PATH
from hadmin import mock


class NodeManagerTest(TestCase):

    def testHealthy(self):
        self.assertEqual(self.rest.isHealthy(), True)

    def testHealthReport(self):
        self.assertEqual(self.rest.getHealthReport(),
                         "1/2 local-dirs are bad: /var/hadoop/compute; ")

    def setUp(self):
        with open('data/nodemanager.rest.json') as f:
            self.rest = NodeManager.load_from_json(f.read())


class NodeManagerNetworkTest(NodeManagerTest):

    def setUp(self):
        self.rest = NodeManager.load_from_connection(mock.RESTConnectionMock(),
                                                     path=NM_INFO_PATH)


class ResourceManagerTest(TestCase):

    def testRootUsed(self):
        self.assertEqual(self.rm.capacity_used, 0.0)

    def testRootMax(self):
        self.assertEqual(self.rm.capacity_max, 100.0)

    def testRootQueeus(self):
        self.assertEqual(len(self.rm.queues), 4)

    def testRootQueueNames(self):
        self.assertEqual(['root.default', 'root.staff', 'root.staff.dev',
                          'root.staff.qa'],
                         self.queue_names)

    def testAppsCompleted(self):
        self.assertEqual(2592, self.rm.apps_completed)

    def testAppsFailed(self):
        self.assertEqual(0, self.rm.apps_failed)

    def testAppsKilled(self):
        self.assertEqual(2, self.rm.apps_killed)

    def testAppsPending(self):
        self.assertEqual(0, self.rm.apps_pending)

    def testAppsRunning(self):
        self.assertEqual(0, self.rm.apps_running)

    def testAppsSubmitted(self):
        self.assertEqual(2598, self.rm.apps_submitted)

    def testNodesActive(self):
        self.assertEqual(12, self.rm.nodes_active)

    def testNodesDecommissioned(self):
        self.assertEqual(11, self.rm.nodes_decommissioned)

    def testNodesTotal(self):
        self.assertEqual(59, self.rm.nodes_total)

    def testNodesUnhealthy(self):
        self.assertEqual(0, self.rm.nodes_unhealthy)

    def testMemAlloc(self):
        self.assertEqual(0, self.rm.memory_mb_allocated)

    def testMemRes(self):
        self.assertEqual(0, self.rm.memory_mb_reserved)

    def testMemTotal(self):
        self.assertEqual(774144, self.rm.memory_mb_total)

    def testCPUsAlloc(self):
        self.assertEqual(0, self.rm.vcpus_allocated)

    def testCPUsRes(self):
        self.assertEqual(0, self.rm.vcpus_reserved)

    def testCPUsTotal(self):
        self.assertEqual(164, self.rm.vcpus_total)

    # 'default' test
    def testDefaultAbsCapUsed(self):
        self.assertEqual(self.default.absolute_capacity_used, 2.0)

    def testDefaultApplications(self):
        self.assertEqual(self.default.applications, 3)

    def testDefaultCapUsed(self):
        self.assertEqual(self.default.capacity_used, 4.0)

    def testDefaultContainers(self):
        self.assertEqual(self.default.containers, 6)

    def testDefaultCpus(self):
        self.assertEqual(self.default.resources_used_vcpus, 6)

    def testDefaultMem(self):
        self.assertEqual(self.default.resources_used_memory_mb, 8192)
    # end 'default' test

    # 'staff' test
    def testStaffAbsCapUsed(self):
        self.assertEqual(self.staff.absolute_capacity_used, 50.0)

    def testStaffApplications(self):
        self.assertEqual(self.staff.applications, 5)

    def testStaffCapUsed(self):
        self.assertEqual(self.staff.capacity_used, 100.0)

    def testStaffContainers(self):
        self.assertEqual(self.staff.containers, 0)

    def testStaffCpus(self):
        self.assertEqual(self.staff.resources_used_vcpus, 20)

    def testStaffMem(self):
        self.assertEqual(self.staff.resources_used_memory_mb, 50000)
    # end 'staff' test

    # 'staff.dev' test
    def testStaffDevAbsCapUsed(self):
        self.assertEqual(self.staff_dev.absolute_capacity_used, 45.0)

    def testStaffDevApplications(self):
        self.assertEqual(self.staff_dev.applications, 3)

    def testStaffDevCapUsed(self):
        self.assertEqual(self.staff_dev.capacity_used, 100.0)

    def testStaffDevContainers(self):
        self.assertEqual(self.staff_dev.containers, 30)

    def testStaffDevCpus(self):
        self.assertEqual(self.staff_dev.resources_used_vcpus, 15)

    def testStaffDevMem(self):
        self.assertEqual(self.staff_dev.resources_used_memory_mb, 40000)
    # end 'staff.dev' test

    def setUp(self):
        met = ''
        sched = ''
        with open('data/resourcemanager.scheduler.json') as f:
            sched = f.read()

        with open('data/resourcemanager.metrics.json') as f:
            met = f.read()

        self.rm = ResourceManager.load_from_jsons([sched, met])
        self.queue_names = []

        for q in self.rm.queues:
            if q.name == 'root.default':
                self.default = q
            elif q.name == 'root.staff':
                self.staff = q
            elif q.name == 'root.staff.dev':
                self.staff_dev = q

            self.queue_names.append(q.name)

        self.queue_names = sorted(self.queue_names)
