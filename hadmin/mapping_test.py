import unittest
from hadmin.mapping import HadoopMapper

class HadoopMapperTest(unittest.TestCase):

    def setUp(self):
        self.mapper = HadoopMapper()

    def tearDown(self):
        self.mapper = None

    def test_mapping_max_jobs_v1(self):
        self.assertEqual(self.mapper['maxjobs', 1],
                'mapred.capacity-scheduler.maximum-system-jobs')

    def test_mapping_max_jobs_v1_with_owner(self):
        self.assertEqual(self.mapper['maxjobs', 1, 'scheduler'],
                'mapred.capacity-scheduler.maximum-system-jobs')

    def test_mapping_max_jobs_v2(self):
        self.assertEqual(self.mapper['maxjobs', 2],
                         'yarn.scheduler.capacity.maximum-applications')

    def test_mapping_max_jobs_v2_with_owner(self):
        self.assertEqual(self.mapper['maxjobs', 2, 'scheduler'],
                         'yarn.scheduler.capacity.maximum-applications')

    def test_state_v1(self):
        with self.assertRaises(KeyError):
            self.mapper['state', 1]

    def test_state_v1_with_owner(self):
        with self.assertRaises(KeyError):
            self.mapper['state', 1, 'queues']

    def test_state_v2(self):
        self.assertEqual(self.mapper['state', 2],
                         'yarn.scheduler.capacity.root.' + HadoopMapper.rep +
                         '.state')

    def test_state_v2_with_owner(self):
        self.assertEqual(self.mapper['state', 2, 'queues'],
                         'yarn.scheduler.capacity.root.' + HadoopMapper.rep +
                         '.state')

    def test_user_limit_factor_v1(self):
        self.assertEqual(self.mapper['ulim', 1],
                         'mapred.capacity-scheduler.default-user-limit-factor')

    def test_user_limit_factor_v1_with_owner(self):
        self.assertEqual(self.mapper['ulim', 1, 'scheduler'],
                         'mapred.capacity-scheduler.default-user-limit-factor')

    def test_user_limit_factor_v2(self):
        self.assertEqual(self.mapper['ulim', 2],
                         'yarn.scheduler.capacity.root.default.user-limit-factor')

    def test_user_limit_factor_v2_with_owner(self):
        self.assertEqual(self.mapper['ulim', 2, 'scheduler'],
                         'yarn.scheduler.capacity.root.default.user-limit-factor')

    def test_max_tpq_v1(self): 
        self.assertEqual(self.mapper['maxtpq', 1],
                         'mapred.capacity-scheduler.default-maximum-active-tasks-per-queue')

    def test_max_tpq_v1_with_owner(self): 
        self.assertEqual(self.mapper['maxtpq', 1, 'scheduler'],
                         'mapred.capacity-scheduler.default-maximum-active-tasks-per-queue')

    def test_max_tpq_v2(self): 
        with self.assertRaises(KeyError):
            self.mapper['maxtpq', 2]

    def test_max_tpq_v2_with_owner(self): 
        with self.assertRaises(KeyError):
            self.mapper['maxtpq', 2, 'scheduler']

    def test_max_tpu_v1_no_owner(self):
        with self.assertRaises(KeyError):
            self.mapper['maxtpu', 1]

    def test_max_tpu_v1_scheduler(self):
        self.assertEqual(self.mapper['maxtpu', 1, 'scheduler'],
                         'mapred.capacity-scheduler.default-maximum-active-tasks-per-user')

    def test_max_tpu_v1_queues(self):
        self.assertEqual(self.mapper['maxtpu', 1, 'queues'],
                         'mapred.capacity-scheduler.queue.' + HadoopMapper.rep + '.maximum-initialized-active-tasks-per-user')

    def test_max_tpu_v2_no_owner(self):
        with self.assertRaises(KeyError):
            self.mapper['maxtpu', 2]

    def test_max_tpu_v2_scheduler(self):
        with self.assertRaises(KeyError):
            self.mapper['maxtpu', 2, 'scheduler']

    def test_max_tpu_v2_queues(self):
        with self.assertRaises(KeyError):
            self.mapper['maxtpu', 2, 'queues']

    def test_admins_v1(self):
        self.assertEqual(self.mapper['admins', 1],
                         'mapred.queue.' + HadoopMapper.rep + '.acl-administer-jobs')

    def test_admins_v1_with_owner(self):
        self.assertEqual(self.mapper['admins', 1, 'queues'],
                         'mapred.queue.' + HadoopMapper.rep + '.acl-administer-jobs')

    def test_admins_v2(self):
        self.assertEqual(self.mapper['admins', 2],
                         'yarn.scheduler.capacity.root.' + HadoopMapper.rep + '.acl_administer_queue')

    def test_admins_v2_with_owner(self):
        self.assertEqual(self.mapper['admins', 2, 'queues'],
                         'yarn.scheduler.capacity.root.' + HadoopMapper.rep + '.acl_administer_queue')

    def test_users_v1(self):
        self.assertEqual(self.mapper['users', 1],
                         'mapred.queue.' + HadoopMapper.rep + '.acl-submit-job')

    def test_users_v1_with_owner(self):
        self.assertEqual(self.mapper['users', 1, 'queues'],
                         'mapred.queue.' + HadoopMapper.rep + '.acl-submit-job')

    def test_users_v2(self):
        self.assertEqual(self.mapper['users', 2],
                         'yarn.scheduler.capacity.root.' + HadoopMapper.rep + '.acl_submit_applications')

    def test_users_v2_with_owner(self):
        self.assertEqual(self.mapper['users', 2, 'queues'],
                         'yarn.scheduler.capacity.root.' + HadoopMapper.rep + '.acl_submit_applications')

    def test_cap_v1(self):
        self.assertEqual(self.mapper['mincap', 1],
                         'mapred.capacity-scheduler.queue.' + HadoopMapper.rep + '.capacity')

    def test_cap_v1_with_owner(self):
        self.assertEqual(self.mapper['mincap', 1, 'queues'],
                         'mapred.capacity-scheduler.queue.' + HadoopMapper.rep + '.capacity')

    def test_cap_v2(self):
        self.assertEqual(self.mapper['mincap', 2],
                         'yarn.scheduler.capacity.root.' + HadoopMapper.rep + '.capacity')

    def test_cap_v2_with_owner(self):
        self.assertEqual(self.mapper['mincap', 2, 'queues'],
                         'yarn.scheduler.capacity.root.' + HadoopMapper.rep + '.capacity')

    def test_max_cap_v1(self):
        self.assertEqual(self.mapper['maxcap', 1],
                         'mapred.capacity-scheduler.queue.' + HadoopMapper.rep + '.maximum-capacity')

    def test_max_cap_v1_with_owner(self):
        self.assertEqual(self.mapper['maxcap', 1, 'queues'],
                         'mapred.capacity-scheduler.queue.' + HadoopMapper.rep + '.maximum-capacity')

    def test_max_cap_v2(self):
        self.assertEqual(self.mapper['maxcap', 2],
                         'yarn.scheduler.capacity.root.' + HadoopMapper.rep + '.maximum-capacity')

    def test_max_cap_v2_with_owner(self):
        self.assertEqual(self.mapper['maxcap', 2, 'queues'],
                         'yarn.scheduler.capacity.root.' + HadoopMapper.rep + '.maximum-capacity')

    def test_versions_supported(self):
        self.assertEqual(self.mapper.versions(), [1, 2])

    def test_version_not_supported(self):
        with self.assertRaises(KeyError):
            self.mapper['maxcap', 3]

    def test_default_init_accept_jobs_factor_v1(self):
        self.assertEqual(
            self.mapper['mapred.capacity-scheduler.default-init-accept-jobs-factor', 1],
            'mapred.capacity-scheduler.default-init-accept-jobs-factor'
            )

    def test_default_init_accept_jobs_factor_v1_with_owner(self):
        self.assertEqual(
            self.mapper['mapred.capacity-scheduler.default-init-accept-jobs-factor', 1, 'scheduler'],
            'mapred.capacity-scheduler.default-init-accept-jobs-factor'
            )

    def test_default_init_accept_jobs_factor_v1_wrong_owner(self):
        with self.assertRaises(KeyError):
            self.mapper['mapred.capacity-scheduler.default-init-accept-jobs-factor', 1, 'queues']

    def test_default_init_accept_jobs_factor_v2(self):
        with self.assertRaises(KeyError):
            self.mapper['mapred.capacity-scheduler.default-init-accept-jobs-factor', 2]

    def test_default_init_accept_jobs_factor_v2_with_owner(self):
        with self.assertRaises(KeyError):
            self.mapper['mapred.capacity-scheduler.default-init-accept-jobs-factor', 2, 'scheduler']

    def test_default_minimum_user_limit_percent_v1(self):
        self.assertEqual(
                self.mapper['mapred.capacity-scheduler.default-minimum-user-limit-percent', 1],
                'mapred.capacity-scheduler.default-minimum-user-limit-percent'
                )

    def test_default_minimum_user_limit_percent_v1_with_owner(self):
        self.assertEqual(
                self.mapper['mapred.capacity-scheduler.default-minimum-user-limit-percent', 1, 'scheduler'],
                'mapred.capacity-scheduler.default-minimum-user-limit-percent'
                )

    def test_default_supports_priority_v1(self):
        self.assertEqual(
                self.mapper['mapred.capacity-scheduler.default-supports-priority', 1],
                'mapred.capacity-scheduler.default-supports-priority'
                )

    def test_init_poll_interval_v1(self):
        self.assertEqual(
                self.mapper['mapred.capacity-scheduler.init-poll-interval', 1],
                'mapred.capacity-scheduler.init-poll-interval'
                )

    def test_init_worker_threads(self):
        self.assertEqual(
                self.mapper['mapred.capacity-scheduler.init-worker-threads', 1],
                'mapred.capacity-scheduler.init-worker-threads'
                )

    def test_maximum_am_resource_percent_v2(self):
        self.assertEqual(
                self.mapper['yarn.scheduler.capacity.maximum-am-resource-percent', 2],
                'yarn.scheduler.capacity.maximum-am-resource-percent'
                )

    def test_maximum_am_resource_percent_v2_with_owner(self):
        self.assertEqual(
                self.mapper['yarn.scheduler.capacity.maximum-am-resource-percent', 2, 'scheduler'],
                'yarn.scheduler.capacity.maximum-am-resource-percent'
                )

    def test_node_locality_delay_v2(self):
        self.assertEqual(
                self.mapper['yarn.scheduler.capacity.node-locality-delay', 2],
                'yarn.scheduler.capacity.node-locality-delay'
                )

    def test_node_locality_delay_v2_with_owner(self):
        self.assertEqual(
                self.mapper['yarn.scheduler.capacity.node-locality-delay', 2, 'scheduler'],
                'yarn.scheduler.capacity.node-locality-delay'
                )

    def test_resource_calculator_v2(self):
        self.assertEqual(
                self.mapper['yarn.scheduler.capacity.resource-calculator', 2],
                'yarn.scheduler.capacity.resource-calculator'
                )

    def test_resource_calculator_v2_with_owner(self):
        self.assertEqual(
                self.mapper['yarn.scheduler.capacity.resource-calculator', 2, 'scheduler'],
                'yarn.scheduler.capacity.resource-calculator'
                )
