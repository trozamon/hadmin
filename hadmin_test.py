import unittest
from hadmin import *


class HadminTest(unittest.TestCase):

    def setUp(self):
        self.base = """<configuration>
\t<property>
\t\t<name>yarn.scheduler.capacity.root.queues</name>
\t\t<value>a,b</value>
\t</property>
\t<property>
\t\t<name>yarn.scheduler.capacity.root.a.capacity</name>
\t\t<value>50</value>
\t</property>
\t<property>
\t\t<name>yarn.scheduler.capacity.root.a.maximum-capacity</name>
\t\t<value>50</value>
\t</property>
\t<property>
\t\t<name>yarn.scheduler.capacity.root.a.acl_submit_applications</name>
\t\t<value>trozamon,root</value>
\t</property>
\t<property>
\t\t<name>yarn.scheduler.capacity.root.a.acl_administer_queue</name>
\t\t<value>root,test,trozamon</value>
\t</property>
\t<property>
\t\t<name>yarn.scheduler.capacity.root.a.user-limit-factor</name>
\t\t<value>25</value>
\t</property>
\t<property>
\t\t<name>yarn.scheduler.capacity.root.a.state</name>
\t\t<value>running</value>
\t</property>
\t<property>
\t\t<name>yarn.scheduler.capacity.root.b.capacity</name>
\t\t<value>50</value>
\t</property>
\t<property>
\t\t<name>yarn.scheduler.capacity.root.b.maximum-capacity</name>
\t\t<value>100</value>
\t</property>
\t<property>
\t\t<name>yarn.scheduler.capacity.root.b.acl_submit_applications</name>
\t\t<value>trozamon,root</value>
\t</property>
\t<property>
\t\t<name>yarn.scheduler.capacity.root.b.acl_administer_queue</name>
\t\t<value>root</value>
\t</property>
\t<property>
\t\t<name>yarn.scheduler.capacity.root.b.user-limit-factor</name>
\t\t<value>25</value>
\t</property>
\t<property>
\t\t<name>yarn.scheduler.capacity.root.b.state</name>
\t\t<value>running</value>
\t</property>
</configuration>"""
        self.xmltree = ET.fromstring(self.base)
        self.hxml = HXML.from_etree(self.xmltree)
        self.man = QueueManager(self.hxml)

    def testQueueFQNBare(self):
        self.assertEqual(queue_fqn('staff'), 'root.staff')

    def testQueueFQNOnFQN(self):
        self.assertEqual(queue_fqn('root.staff'), 'root.staff')

    def testQueueFQNEmpty(self):
        self.assertEqual(queue_fqn(), 'root')

    def testQueueParentOneLevel(self):
        self.assertEqual(queue_parent('staff'), 'root')

    def testQueueParentTwoLevel(self):
        self.assertEqual(queue_parent('staff.hpc'), 'root.staff')

    def testQueueParentTwoLevelFQN(self):
        self.assertEqual(queue_parent('root.staff.hpc'), 'root.staff')

    def testQueueCapacityFQN(self):
        self.assertEqual(queue_cap_fqn(queue_fqn('staff')),
                         'yarn.scheduler.capacity.root.staff.capacity')

    def testQueueMaximumCapacityFQN(self):
        self.assertEqual(queue_maxcap_fqn(queue_fqn('staff')),
                         'yarn.scheduler.capacity.root.staff.maximum-capacity')

    def testQueueStateFQN(self):
        self.assertEqual(queue_state_fqn(queue_fqn('staff')),
                         'yarn.scheduler.capacity.root.staff.state')

    def testQueueULimFQN(self):
        self.assertEqual(queue_ulim_fqn(queue_fqn('staff')),
                         'yarn.scheduler.capacity.root.staff.user-limit-factor'
                         )

    def testQueueUsersFQN(self):
        self.assertEqual(queue_users_fqn(queue_fqn('staff')),
                         'yarn.scheduler.capacity.root.staff.' +
                         'acl_submit_applications')

    def testQueueAdminsFQN(self):
        self.assertEqual(queue_admins_fqn(queue_fqn('staff')),
                         'yarn.scheduler.capacity.root.staff.' +
                         'acl_administer_queue')

    def testQueueQueueSubsFQN(self):
        self.assertEqual(queue_subs_fqn(queue_fqn('staff')),
                         'yarn.scheduler.capacity.root.staff.queues')

    def testQueueCapacity(self):
        self.assertEqual(queue_cap_fqn('staff'),
                         'yarn.scheduler.capacity.root.staff.capacity')

    def testQueueMaximumCapacity(self):
        self.assertEqual(queue_maxcap_fqn('staff'),
                         'yarn.scheduler.capacity.root.staff.maximum-capacity')

    def testQueueState(self):
        self.assertEqual(queue_state_fqn('staff'),
                         'yarn.scheduler.capacity.root.staff.state')

    def testQueueULim(self):
        self.assertEqual(queue_ulim_fqn('staff'),
                         'yarn.scheduler.capacity.root.staff.user-limit-factor'
                         )

    def testQueueUsers(self):
        self.assertEqual(queue_users_fqn('staff'),
                         'yarn.scheduler.capacity.root.staff.' +
                         'acl_submit_applications')

    def testQueueAdmins(self):
        self.assertEqual(queue_admins_fqn('staff'),
                         'yarn.scheduler.capacity.root.staff.' +
                         'acl_administer_queue')

    def testQueueQueueSubs(self):
        self.assertEqual(queue_subs_fqn('staff'),
                         'yarn.scheduler.capacity.root.staff.queues')

    def testQueueQueueSubsFQNEmptyArg(self):
        self.assertEqual(queue_subs_fqn(),
                         'yarn.scheduler.capacity.root.queues')

    def testHXMLGetter(self):
        self.assertEqual(self.hxml[queue_users_fqn('a')], 'trozamon,root')

    def testHXMLSetter(self):
        self.hxml[queue_users_fqn('a')] = 'hehe'
        self.assertEqual(self.hxml[queue_users_fqn('a')], 'hehe')

    def testAddUserToQueue(self):
        self.man.add_user('test', 'a')
        self.assertEqual(self.hxml[queue_users_fqn('a')], 'root,test,trozamon')

    def testRemoveUserFromQueue(self):
        self.man.del_user('root', 'a')
        self.assertEqual(self.hxml[queue_users_fqn('a')], 'trozamon')

    def testAddAdminToQueue(self):
        self.man.add_admin('test', 'a')
        self.assertEqual(self.hxml[queue_admins_fqn('a')],
                'root,test,trozamon')

    def testRemoveAdminFromQueue(self):
        self.man.del_admin('root', 'a')
        self.assertEqual(self.hxml[queue_admins_fqn('a')], 'test,trozamon')

    def testAddQueueStaffInRootSubs(self):
        self.man.add('staff', 'trozamon')
        self.assertEqual(self.hxml[queue_subs_fqn()], 'a,b,staff')

    def testAddQueueStaffHasAdmin(self):
        self.man.add('staff', 'trozamon')
        self.assertEqual(self.hxml[queue_admins_fqn('staff')], 'trozamon')

    def testAddQueueStaffHasUser(self):
        self.man.add('staff', 'trozamon')
        self.assertEqual(self.hxml[queue_users_fqn('staff')], 'trozamon')

    def testAddQueueStaffHasCapacity(self):
        self.man.add('staff', 'trozamon')
        self.assertEqual(self.hxml[queue_cap_fqn('staff')], '0')

    def testAddQueueStaffHasMaximumCapacity(self):
        self.man.add('staff', 'trozamon')
        self.assertEqual(self.hxml[queue_maxcap_fqn('staff')], '100')

    def testAddQueueStaffHasUlim(self):
        self.man.add('staff', 'trozamon')
        self.assertEqual(self.hxml[queue_ulim_fqn('staff')], '25')

    def testAddQueueStaffIsRunning(self):
        self.man.add('staff', 'trozamon')
        self.assertEqual(self.hxml[queue_state_fqn('staff')], 'running')

    def testRemoveQueue(self):
        self.man.delete('b')
        self.assertEqual(self.hxml[queue_subs_fqn()], 'a')

    def testRemoveQueueNoMoreUsers(self):
        self.man.delete('b')
        with self.assertRaises(KeyError):
            self.hxml[queue_users_fqn('b')]

    def testRemoveQueueNoMoreAdmins(self):
        self.man.delete('b')
        with self.assertRaises(KeyError):
            self.hxml[queue_admins_fqn('b')]

    def testRemoveQueueNoMoreState(self):
        self.man.delete('b')
        with self.assertRaises(KeyError):
            self.hxml[queue_state_fqn('b')]

    def testRemoveQueueNoMoreUlim(self):
        self.man.delete('b')
        with self.assertRaises(KeyError):
            self.hxml[queue_ulim_fqn('b')]

    def testRemoveQueueNoMoreCap(self):
        self.man.delete('b')
        with self.assertRaises(KeyError):
            self.hxml[queue_cap_fqn('b')]

    def testRemoveQueueNoMoreMaxcap(self):
        self.man.delete('b')
        with self.assertRaises(KeyError):
            self.hxml[queue_maxcap_fqn('b')]
