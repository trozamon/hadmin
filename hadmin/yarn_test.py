from unittest2 import TestCase
from hadmin.yarn import CapacityScheduler, Queue


class CapacitySchedulerTest(TestCase):

    def testManagerQueueList(self):
        self.assertEqual(['root', 'root.a', 'root.b'], self.man.queue_list())

    def testAddUserToQueue(self):
        tmp = 'yarn.scheduler.capacity.root.a.acl_submit_applications'
        self.man.queue('a').users.append('test')
        self.assertEqual(self.man.to_hxml()[tmp], 'root,test,trozamon')

    def testRemoveUserFromQueue(self):
        tmp = 'yarn.scheduler.capacity.root.a.acl_submit_applications'
        self.man.queue('a').users.remove('root')
        self.assertEqual(self.man.to_hxml()[tmp], 'trozamon')

    def testAddAdminToQueue(self):
        tmp = 'yarn.scheduler.capacity.root.a.acl_administer_queue'
        self.man.queue('a').admins.append('test')
        self.assertEqual(self.man.to_hxml()[tmp], 'root,test,trozamon')

    def testRemoveAdminFromQueue(self):
        tmp = 'yarn.scheduler.capacity.root.a.acl_administer_queue'
        self.man.queue('a').admins.remove('root')
        self.assertEqual(self.man.to_hxml()[tmp], 'test,trozamon')

    def testAddQueueStaffInRootSubs(self):
        tmp = 'yarn.scheduler.capacity.root.queues'
        q = Queue(name='staff', admins=['alec'], users=['alec', 'trozamon'])
        self.man.root_queue.subqueues.append(q)
        self.assertEqual(self.man.to_hxml()[tmp], 'a,b,staff')

    def testCapacityCheckSuccess(self):
        self.assertEqual(0, len(self.man.check_capacities()))

    def testCapacityCheckFailure(self):
        q = Queue(name='staff', admins=['alec'], users=['alec'])
        q.cap_min = 1
        self.man.root_queue.subqueues.append(q)
        self.assertEqual(1, len(self.man.check_capacities()))

    def testMaximumCapacitiesCheckSuccess(self):
        self.assertEqual(0, len(self.man.check_maximum_capacities()))

    def testMaximumCapacitiesCheckFailure(self):
        q = Queue(name='staff', admins=['alec'], users=['alec'])
        q.cap_min = 2
        q.cap_max = 1
        self.man.root_queue.subqueues.append(q)
        self.assertEqual(1, len(self.man.check_maximum_capacities()))

    def setUp(self):
        self.man = CapacityScheduler.from_file('data/capacity-scheduler.xml')


class QueueTest(TestCase):

    def setUp(self):
        self.root = Queue(name='root')

    def testFQNUsers(self):
        self.assertEqual(
                Queue.fqn_users('root.a'),
                'yarn.scheduler.capacity.root.a.acl_submit_applications'
                )

    def testFQNAdmins(self):
        self.assertEqual(
                Queue.fqn_admins('root.a'),
                'yarn.scheduler.capacity.root.a.acl_administer_queue'
                )

    def testFQNCap(self):
        self.assertEqual(
                Queue.fqn_cap('root.a'),
                'yarn.scheduler.capacity.root.a.capacity'
                )

    def testFQNMaxCap(self):
        self.assertEqual(
                Queue.fqn_maxcap('root.a'),
                'yarn.scheduler.capacity.root.a.maximum-capacity'
                )

    def testFQNUlim(self):
        self.assertEqual(
                Queue.fqn_ulim('root.a'),
                'yarn.scheduler.capacity.root.a.user-limit-factor'
                )

    def testFQNState(self):
        self.assertEqual(
                Queue.fqn_state('root.a'),
                'yarn.scheduler.capacity.root.a.state'
                )

    def testFQNSubs(self):
        self.assertEqual(
                Queue.fqn_subs('root.a'),
                'yarn.scheduler.capacity.root.a.queues'
                )

    def testSetBCapacityInt(self):
        self.root.cap_min = 100
        hxml = self.root.to_hxml()
        self.assertEqual('100.0', hxml[Queue.fqn_cap('root')])

    def testSetBMaximumCapacityFloat(self):
        self.root.cap_max = 5.1
        hxml = self.root.to_hxml()
        self.assertEqual('5.1', hxml[Queue.fqn_maxcap('root')])

    def testSetCapacityGreaterThan100(self):
        with self.assertRaises(ValueError):
            self.root.cap_min = 101

    def testSetCapacityLessThan0(self):
        with self.assertRaises(ValueError):
            self.root.cap_min = -1

    def testSetMaximumCapacityGreaterThan100(self):
        with self.assertRaises(ValueError):
            self.root.cap_max = 101

    def testSetMaximumCapacityLessThan0(self):
        with self.assertRaises(ValueError):
            self.root.cap_max = -1

    def testSetOff(self):
        self.root.running = False
        hxml = self.root.to_hxml()
        self.assertEqual('STOPPED', hxml[Queue.fqn_state('root')])

    def testSetOn(self):
        self.root.running = False
        self.root.running = True
        hxml = self.root.to_hxml()
        self.assertEqual('RUNNING', hxml[Queue.fqn_state('root')])

    def testSetUlimLessThanZero(self):
        with self.assertRaises(ValueError):
            self.root.user_limit_factor = -1

    def testSetUlimLessThanZeroStr(self):
        with self.assertRaises(ValueError):
            self.root.user_limit_factor = '-1'
