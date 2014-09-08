import unittest
from hadmin import *


class HadminTest(unittest.TestCase):

    def testManSanityCheckUsers(self):
        self.assertEqual(['trozamon'], self.man.sc_users(self.passwd))

    def testManSanityCheckAdmins(self):
        self.assertEqual(['test', 'trozamon'], self.man.sc_admins(self.passwd))

    def testManAdminListB(self):
        self.man.add('b.b', 'blah')
        self.assertEqual(self.man.admin_list('b'),
                ['blah', 'root'])

    def testManAdminListB(self):
        self.assertEqual(self.man.admin_list('b'),
                ['root'])

    def testManUserListAAfterAdding(self):
        self.man.add('a.a', 'blah')
        self.assertEqual(self.man.user_list('a'),
                ['blah', 'root', 'trozamon'])

    def testManUserListRoot(self):
        self.assertEqual(self.man.user_list(),
                ['root', 'trozamon'])

    def testUserListFromPasswd(self):
        self.assertEqual(users_from_passwd(self.passwd),
                ['adm', 'root'])

    def testManagerAddQueueMultipleUsersThrows(self):
        with self.assertRaises(ValueError):
            self.man.add('superqueue', 'bill,alan')

    def testManagerAddMultipleUsers(self):
        self.man.add_user('bob,bill', 'a')
        self.assertEqual(self.hxml[queue_users_fqn('a')],
                'bill,bob,root,trozamon')

    def testManagerAddMultipleAdmins(self):
        self.man.add_admin('zoe,joebob', 'a')
        self.assertEqual(self.hxml[queue_admins_fqn('a')],
                'joebob,root,test,trozamon,zoe')

    def testManagerAddAlreadyExistingQueue(self):
        with self.assertRaises(KeyError):
            self.man.add('a', 'trozamon')

    def testManagerSanityCheckMaximumCapacitiesNoProbs(self):
        self.assertEqual(self.man.sc_maxcaps(), [])

    def testManagerSanityCheckMaximumCapacitiesProb(self):
        self.man.add('a.a', 'trozamon')
        self.man.set_cap('a.a', 50)
        self.man.set_maxcap('a.a', 25)
        self.assertEqual(self.man.sc_maxcaps(), ['root.a.a'])

    def testManagerSanityCheckCapacitiesNoProbs(self):
        self.assertEqual(self.man.sc_caps(), [])

    def testManagerSanityCheckCapacitiesProb(self):
        self.man.add('a.a', 'trozamon')
        self.man.add('a.b', 'trozamon')
        self.man.add('b.a', 'trozamon')
        self.man.add('b.b', 'trozamon')
        self.assertEqual(self.man.sc_caps(), ['root.a', 'root.b'])

    def testManagerQueueList(self):
        self.assertEqual(self.man.queue_list(), ['root', 'root.a', 'root.b'])

    def testManagerQueueListA(self):
        self.assertEqual(self.man.queue_list('a'), ['root.a'])

    def testManagerQueueListWithTwoLevelSub(self):
        self.man.add('a.staff', 'trozamon')
        self.assertEqual(self.man.queue_list(),
                ['root', 'root.a', 'root.a.staff', 'root.b'])

    def testManagerQueueListAWithTwoLevelSub(self):
        self.man.add('a.staff', 'trozamon')
        self.assertEqual(self.man.queue_list('a'), ['root.a', 'root.a.staff'])

    def testManagerQueueListStaffWithTwoLevelSub(self):
        self.man.add('a.staff', 'trozamon')
        self.assertEqual(self.man.queue_list('a.staff'), ['root.a.staff'])

    def testManagerQueueListBadQueue(self):
        with self.assertRaises(KeyError):
            self.man.queue_list('trozamon')

    def testSetBCapacityInt(self):
        self.man.set_cap('b', 100)
        self.assertEqual(self.hxml[queue_cap_fqn('b')], '100')

    def testSetBMaximumCapacityFloat(self):
        self.man.set_maxcap('b', 5.1)
        self.assertEqual(self.hxml[queue_maxcap_fqn('b')], '5')

    def testSetBCapacityGreaterThan100(self):
        with self.assertRaises(ValueError):
            self.man.set_cap('b', 101)

    def testSetBCapacityLessThan0(self):
        with self.assertRaises(ValueError):
            self.man.set_cap('b', -1)

    def testSetBMaximumCapacityGreaterThan100(self):
        with self.assertRaises(ValueError):
            self.man.set_maxcap('b', 101)

    def testSetBMaximumCapacityLessThan0(self):
        with self.assertRaises(ValueError):
            self.man.set_maxcap('b', -1)

    def testSetBOff(self):
        self.man.off('b')
        self.assertEqual(self.hxml[queue_state_fqn('b')], 'stopped')

    def testSetBOn(self):
        self.man.on('b')
        self.assertEqual(self.hxml[queue_state_fqn('b')], 'running')

    def testSetUlimGreaterThanOne(self):
        with self.assertRaises(ValueError):
            self.man.set_ulim('b', 2)

    def testSetUlimGreaterThanOneStr(self):
        with self.assertRaises(ValueError):
            self.man.set_ulim('b', '2')

    def testSetUlimLessThanOne(self):
        with self.assertRaises(ValueError):
            self.man.set_ulim('b', -1)

    def testSetUlimLessThanOneStr(self):
        with self.assertRaises(ValueError):
            self.man.set_ulim('b', '-1')

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

    def testHXMLGetterNonExistent(self):
        with self.assertRaises(KeyError):
            self.hxml['roar']

    def testHXMLSetter(self):
        self.hxml[queue_users_fqn('a')] = 'hehe'
        self.assertEqual(self.hxml[queue_users_fqn('a')], 'hehe')

    def testHXMLSetterNonExistent(self):
        self.hxml['hehe'] = 'hey'
        self.assertEqual(self.hxml['hehe'], 'hey')

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

    def testAddQueueStaffBossmanInStaffSubs(self):
        self.man.add('staff', 'trozamon')
        self.man.add('staff.bossman', 'trozamon')
        self.assertEqual(self.hxml[queue_subs_fqn('staff')], 'bossman')

    def testAddQueueStaffBossmanNotInRootSubs(self):
        self.man.add('staff', 'trozamon')
        self.man.add('staff.bossman', 'trozamon')
        self.assertFalse('bossman' in self.hxml[queue_subs_fqn()].split(','))

    def testAddQueueStaffHasAdmin(self):
        self.man.add('staff', 'trozamon')
        self.assertEqual(self.hxml[queue_admins_fqn('staff')], 'trozamon')

    def testAddQueueStaffHasUser(self):
        self.man.add('staff', 'trozamon')
        self.assertEqual(self.hxml[queue_users_fqn('staff')], 'trozamon')

    def testAddQueueStaffHasCapacity(self):
        self.man.add('staff', 'trozamon')
        self.assertEqual(self.hxml[queue_cap_fqn('staff')], '1')

    def testAddQueueStaffHasMaximumCapacity(self):
        self.man.add('staff', 'trozamon')
        self.assertEqual(self.hxml[queue_maxcap_fqn('staff')], '100')

    def testAddQueueStaffHasUlim(self):
        self.man.add('staff', 'trozamon')
        self.assertEqual(self.hxml[queue_ulim_fqn('staff')], '0.25')

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

    def setUp(self):
        self.passwd = """root:x:0:0:root:/root:/bin/zsh
adm:x:3:4:adm:/var/adm:/bin/false
"""

        self.base = """<configuration>
\t<property>
\t\t<name>yarn.scheduler.capacity.root.queues</name>
\t\t<value>a,b</value>
\t</property>
\t<property>
\t\t<name>yarn.scheduler.capacity.root.capacity</name>
\t\t<value>100</value>
\t</property>
\t<property>
\t\t<name>yarn.scheduler.capacity.root.maximum-capacity</name>
\t\t<value>100</value>
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
