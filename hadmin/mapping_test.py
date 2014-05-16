import unittest
from hadmin.mapping import HadoopMapper

class HadoopMapperTest(unittest.TestCase):

    def setUp(self):
        self.mapper = HadoopMapper()

    def tearDown(self):
        self.mapper = None

    def test_mapping_max_jobs_v1(self):
        self.assertEqual(self.mapper['max-jobs', 1],
                'mapred.capacity-scheduler.maximum-system-jobs')

    def test_mapping_max_jobs_v1_with_owner(self):
        self.assertEqual(self.mapper['max-jobs', 1, 'scheduler'],
                'mapred.capacity-scheduler.maximum-system-jobs')

    def test_mapping_max_jobs_v2(self):
        self.assertEqual(self.mapper['max-jobs', 2],
                         'yarn.scheduler.capacity.maximum-applications')

    def test_mapping_max_jobs_v2_with_owner(self):
        self.assertEqual(self.mapper['max-jobs', 2, 'scheduler'],
                         'yarn.scheduler.capacity.maximum-applications')

    def test_user_limit_factor_v1(self):
        self.assertEqual(self.mapper['user-limit-factor', 1],
                         'mapred.capacity-scheduler.default-user-limit-factor')

    def test_user_limit_factor_v1_with_owner(self):
        self.assertEqual(self.mapper['user-limit-factor', 1, 'scheduler'],
                         'mapred.capacity-scheduler.default-user-limit-factor')

    def test_user_limit_factor_v2(self):
        self.assertEqual(self.mapper['user-limit-factor', 2],
                         'yarn.scheduler.capacity.root.default.user-limit-factor')

    def test_user_limit_factor_v2_with_owner(self):
        self.assertEqual(self.mapper['user-limit-factor', 2, 'scheduler'],
                         'yarn.scheduler.capacity.root.default.user-limit-factor')

    def test_max_tpq_v1(self): 
        self.assertEqual(self.mapper['max-tpq', 1],
                         'mapred.capacity-scheduler.default-maximum-active-tasks-per-queue')

    def test_max_tpq_v1_with_owner(self): 
        self.assertEqual(self.mapper['max-tpq', 1, 'scheduler'],
                         'mapred.capacity-scheduler.default-maximum-active-tasks-per-queue')

    def test_max_tpq_v2(self): 
        with self.assertRaises(KeyError):
            self.mapper['max-tpq', 2]

    def test_max_tpq_v2_with_owner(self): 
        with self.assertRaises(KeyError):
            self.mapper['max-tpq', 2, 'scheduler']

    def test_max_tpu_v1_no_owner(self):
        with self.assertRaises(KeyError):
            self.mapper['max-tpu', 1]

    def test_max_tpu_v1_scheduler(self):
        self.assertEqual(self.mapper['max-tpu', 1, 'scheduler'],
                         'mapred.capacity-scheduler.default-maximum-active-tasks-per-user')

    def test_max_tpu_v1_queues(self):
        self.assertEqual(self.mapper['max-tpu', 1, 'queues'],
                         'mapred.capacity-scheduler.queue.' + HadoopMapper.rep + '.maximum-initialized-active-tasks-per-user')

    def test_max_tpu_v2_no_owner(self):
        with self.assertRaises(KeyError):
            self.mapper['max-tpu', 2]

    def test_max_tpu_v2_scheduler(self):
        with self.assertRaises(KeyError):
            self.mapper['max-tpu', 2, 'scheduler']

    def test_max_tpu_v2_queues(self):
        with self.assertRaises(KeyError):
            self.mapper['max-tpu', 2, 'queues']

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
        self.assertEqual(self.mapper['cap', 1],
                         'mapred.capacity-scheduler.queue.' + HadoopMapper.rep + '.capacity')

    def test_cap_v1_with_owner(self):
        self.assertEqual(self.mapper['cap', 1, 'scheduler'],
                         'mapred.capacity-scheduler.queue.' + HadoopMapper.rep + '.capacity')

    def test_cap_v2(self):
        self.assertEqual(self.mapper['cap', 2],
                         'yarn.scheduler.capacity.root.' + HadoopMapper.rep + '.capacity')

    def test_cap_v2_with_owner(self):
        self.assertEqual(self.mapper['cap', 2, 'scheduler'],
                         'yarn.scheduler.capacity.root.' + HadoopMapper.rep + '.capacity')

    def test_max_cap_v1(self):
        self.assertEqual(self.mapper['max-cap', 1],
                         'mapred.capacity-scheduler.queue.' + HadoopMapper.rep + '.maximum-capacity')

    def test_max_cap_v1_with_owner(self):
        self.assertEqual(self.mapper['max-cap', 1, 'scheduler'],
                         'mapred.capacity-scheduler.queue.' + HadoopMapper.rep + '.maximum-capacity')

    def test_max_cap_v2(self):
        self.assertEqual(self.mapper['max-cap', 2],
                         'yarn.scheduler.capacity.root.' + HadoopMapper.rep + '.maximum-capacity')

    def test_max_cap_v2_with_owner(self):
        self.assertEqual(self.mapper['max-cap', 2, 'scheduler'],
                         'yarn.scheduler.capacity.root.' + HadoopMapper.rep + '.maximum-capacity')

    def test_versions_supported(self):
        self.assertEqual(self.mapper.versions(), [1, 2])

    def test_version_not_supported(self):
        with self.assertRaises(KeyError):
            self.mapper['max-cap', 3]
