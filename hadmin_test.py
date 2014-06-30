import unittest
from hadmin import *


class HadminTest(unittest.TestCase):

    def testQueueFQNBare(self):
        self.assertEqual(queue_fqn('staff'), 'root.staff')

    def testQueueFQNOnFQN(self):
        self.assertEqual(queue_fqn('root.staff'), 'root.staff')

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
