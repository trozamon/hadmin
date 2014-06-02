import unittest
import copy
import re
import pkgutil
import yaml
from hadmin.config import Config, Internal, TypeChecker


class ConfigTest(unittest.TestCase):

    def setUp(self):
        self.conf = Config()
        self.data = {
            'key1': {
                'value': 'foo',
                'final': False
                },
            'key2': {
                'value': 'hey',
                'final': True
                },
            'key3': {
                'value': 'sup',
                'final': False
                }
            }
        self.invdata = copy.deepcopy(self.data)
        del(self.invdata['key1']['final'])
        self.throwone = copy.deepcopy(self.data)
        self.throwone['key1'] = {'val': 'hey', 'final': False}

    def tearDown(self):
        self.conf = None
        self.data = None
        self.invdata = None
        self.throwone = None

    def test_fixup(self):
        self.conf.fixup(self.invdata)
        self.assertTrue(self.data == self.invdata)

    def test_fixup_throws(self):
        with self.assertRaises(KeyError):
            self.conf.fixup(self.throwone)

    def test_add(self):
        self.conf.add(self.data)

    def test_str_value_only(self):
        self.conf['test'] = 1
        self.assertTrue(str(self.conf) == "\ttest:\t1")

    def test_str_final_only(self):
        self.conf['test', Config.fnl_tag] = True
        self.assertTrue(str(self.conf) == "final\ttest:\t")

    def test_set_item_value_only(self):
        self.conf['test'] = 'value'
        self.assertTrue(self.conf['test'] == 'value')

    def test_set_item_final_only(self):
        self.conf['test1', Config.fnl_tag] = True
        self.assertTrue(self.conf['test1', Config.fnl_tag])
        self.conf['test1', Config.fnl_tag] = False
        self.assertFalse(self.conf['test1', Config.fnl_tag])

    def test_to_xml(self):
        ans = """<configuration>
\t<property>
\t\t<name>key1</name>
\t\t<value>foo</value>
\t</property>
\t<property>
\t\t<name>key2</name>
\t\t<value>hey</value>
\t\t<final>true</final>
\t</property>
\t<property>
\t\t<name>key3</name>
\t\t<value>sup</value>
\t</property>
</configuration>"""
        self.conf.add(self.data)
        self.assertEqual(self.conf.to_xml(), ans)


class InternalTest(unittest.TestCase):

    def setUp(self):
        self.data = {
            'queues': {
                'tester': {
                    'admins': 'bossman',
                    'mincap': 50,
                    'maxcap': 60,
                    'maxtpu': 1000,
                    'state': 'running',
                    'ulim': 0.1,
                    'users': 'bossman,trozamon'
                    },
                'default': {
                    'admins': 'trozamon',
                    'mincap': 5,
                    'maxcap': 10,
                    'maxtpu': 1000,
                    'state': 'running',
                    'ulim': 0.1,
                    'users': 'root,trozamon'
                    }
                },
            'scheduler': {
                'maxjobs': 100,
                'maxtpq': 1000,
                'maxtpu': 1000,
                'ulim': 10,
                'yarn.scheduler.capacity.maximum-am-resource-percent': 0.1,
                'mapred.capacity-scheduler.default-init-accept-jobs-factor': 5,
                'yarn.scheduler.capacity.node-locality-delay': 5,
                'mapred.capacity-scheduler.default-user-limit-factor':
                    5,
                'yarn.scheduler.capacity.resource-calculator': 'blah',
                'mapred.capacity-scheduler.default-supports-priority': 'true',
                'mapred.capacity-scheduler.init-poll-interval': 5000,
                'mapred.capacity-scheduler.init-worker-threads': 5000
                }
            }
        self.mgr = Internal(self.data)

    def tearDown(self):
        self.mgr = None
        self.data = None

    def test_queue_list(self):
        self.assertEqual(self.mgr.queue_list(), ['default', 'tester'])

    def test_queue_list_str(self):
        self.assertEqual(self.mgr.queue_list_str(), "default,tester")

    def test_add_admin(self):
        self.mgr.add_admin('fluffy', 'default')
        self.assertEqual(self.mgr.conf['queues']['default']['admins'],
                        'fluffy,trozamon')

    def test_add_queue(self):
        self.mgr.add_queue('test', 'trozamon')
        self.assertEqual(self.mgr.conf['queues']['test']['admins'], 'trozamon')
        self.assertEqual(self.mgr.conf['queues']['test']['users'], 'trozamon')
        self.assertEqual(self.mgr.conf['queues']['test']['mincap'], 0)
        self.assertEqual(self.mgr.conf['queues']['test']['maxcap'], 0)
        self.assertEqual(self.mgr.conf['queues']['test']['maxtpu'], 0)

    def test_add_user(self):
        self.mgr.add_user('fluffy', 'default')
        self.assertEqual(self.mgr.conf['queues']['default']['users'],
                        'fluffy,root,trozamon')

    def test_add_user_with(self):
        with Internal(self.data) as mgr:
            mgr.add_user('fluffy', 'default')
            self.assertEqual(mgr.conf['queues']['default']['users'],
                            'fluffy,root,trozamon')

    def test_check_queue(self):
        self.mgr.check_queue('default')
        with self.assertRaises(KeyError):
            self.mgr.check_queue('test')

    def test_del_admin(self):
        self.mgr.add_admin('fluffy', 'default')
        self.mgr.del_admin('trozamon', 'default')
        self.assertEqual(self.mgr.conf['queues']['default']['admins'],
                'fluffy')

    def test_del_queue(self):
        self.mgr.del_queue('default')
        with self.assertRaises(KeyError):
            self.mgr.conf['queues']['default']

    def test_del_user(self):
        self.mgr.del_user('trozamon', 'default')
        self.assertEqual(self.mgr.conf['queues']['default']['users'], 'root')

    def test_set_queue_cap(self):
        self.mgr.set_queue_cap('default', 10)
        self.assertEqual(self.mgr.conf['queues']['default']['mincap'], 10)

    def test_set_queue_max_cap(self):
        self.mgr.set_queue_max_cap('default', 10)
        self.assertEqual(self.mgr.conf['queues']['default']['maxcap'], 10)

    def test_set_queue_max_init_tpu(self):
        self.mgr.set_queue_max_init_tpu('default', 10)
        self.assertEqual(self.mgr.conf['queues']['default']['maxtpu'], 10)

    def test_get_data_queues_v1_admins_tester(self):
        conf = self.mgr.get_data('queues', 1)
        self.assertEqual(conf['queues']['tester']['admins'], 'bossman')

    def test_get_data_queues_v1_users_tester(self):
        conf = self.mgr.get_data('queues', 1)
        self.assertEqual(conf['queues']['tester']['users'], 'bossman,trozamon')

    def test_get_data_queues_v1_admins(self):
        conf = self.mgr.get_data('queues', 1)
        self.assertEqual(conf['queues']['default']['admins'], 'trozamon')

    def test_get_data_queues_v1_users(self):
        conf = self.mgr.get_data('queues', 1)
        self.assertEqual(conf['queues']['default']['users'], 'root,trozamon')

    def test_get_config_queues_v1_users_tester(self):
        conf = self.mgr.get_config('queues', 1)
        self.assertEqual(conf['mapred.queue.tester.acl-submit-job'],
                'bossman,trozamon')

    def test_get_config_queues_v1_admins_tester(self):
        conf = self.mgr.get_config('queues', 1)
        self.assertEqual(conf['mapred.queue.tester.acl-administer-jobs'],
                'bossman')

    def test_get_config_queues_v1_users(self):
        conf = self.mgr.get_config('queues', 1)
        self.assertEqual(conf['mapred.queue.default.acl-submit-job'],
                'root,trozamon')

    def test_get_config_queues_v1_admins(self):
        conf = self.mgr.get_config('queues', 1)
        self.assertEqual(conf['mapred.queue.default.acl-administer-jobs'],
                'trozamon')

    def test_get_data_queues_v2(self):
        conf = self.mgr.get_data('queues', 2)
        self.assertEqual(len(conf), 2)

    def test_get_config_queues_v2(self):
        conf = self.mgr.get_config('queues', 2)

    def test_get_data_scheduler_v1_mincap_tester(self):
        conf = self.mgr.get_data('scheduler', 1)
        self.assertEqual(conf['queues']['tester']['mincap'], 50)

    def test_get_data_scheduler_v1_maxcap_tester(self):
        conf = self.mgr.get_data('scheduler', 1)
        self.assertEqual(conf['queues']['tester']['maxcap'], 60)

    def test_get_data_scheduler_v1_mincap(self):
        conf = self.mgr.get_data('scheduler', 1)
        self.assertEqual(conf['queues']['default']['mincap'], 5)

    def test_get_data_scheduler_v1_maxcap(self):
        conf = self.mgr.get_data('scheduler', 1)
        self.assertEqual(conf['queues']['default']['maxcap'], 10)

    def test_get_data_scheduler_v1_maxjobs(self):
        conf = self.mgr.get_data('scheduler', 1)
        self.assertEqual(conf['scheduler']['maxjobs'], 100)

    def test_get_data_scheduler_v1_ulim(self):
        conf = self.mgr.get_data('scheduler', 1)
        self.assertEqual(conf['scheduler']['ulim'], 10)

    def test_get_config_scheduler_v1(self):
        conf = self.mgr.get_config('scheduler', 1)
        self.assertEqual(conf['mapred.capacity-scheduler.maximum-system-jobs'],
                '100')
        self.assertEqual(conf['mapred.capacity-scheduler.queue.tester.capacity'],
                '50')

    def test_get_data_scheduler_v2_mincap_tester(self):
        conf = self.mgr.get_data('scheduler', 2)
        self.assertEqual(conf['queues']['tester']['mincap'], 50)

    def test_get_data_scheduler_v2_maxcap_tester(self):
        conf = self.mgr.get_data('scheduler', 2)
        self.assertEqual(conf['queues']['tester']['maxcap'], 60)

    def test_get_data_scheduler_v2_mincap(self):
        conf = self.mgr.get_data('scheduler', 2)
        self.assertEqual(conf['queues']['default']['mincap'], 5)

    def test_get_data_scheduler_v2_maxcap(self):
        conf = self.mgr.get_data('scheduler', 2)
        self.assertEqual(conf['queues']['default']['maxcap'], 10)

    def test_get_data_scheduler_v2_admins_tester(self):
        conf = self.mgr.get_data('scheduler', 2)
        self.assertEqual(conf['queues']['tester']['admins'], 'bossman')

    def test_get_data_scheduler_v2_users_tester(self):
        conf = self.mgr.get_data('scheduler', 2)
        self.assertEqual(conf['queues']['tester']['users'], 'bossman,trozamon')

    def test_get_data_scheduler_v2_admins(self):
        conf = self.mgr.get_data('scheduler', 2)
        self.assertEqual(conf['queues']['default']['admins'], 'trozamon')

    def test_get_data_scheduler_v2_users(self):
        conf = self.mgr.get_data('scheduler', 2)
        self.assertEqual(conf['queues']['default']['users'], 'root,trozamon')

    def test_get_data_scheduler_v2_ulim(self):
        conf = self.mgr.get_data('scheduler', 2)
        self.assertEqual(conf['queues']['default']['ulim'], 0.1)

    def test_get_data_scheduler_v2_ulim_tester(self):
        conf = self.mgr.get_data('scheduler', 2)
        self.assertEqual(conf['queues']['tester']['ulim'], 0.1)

    def test_get_data_scheduler_v2_maxjobs(self):
        conf = self.mgr.get_data('scheduler', 2)
        self.assertEqual(conf['scheduler']['maxjobs'], 100)

    def test_get_config_scheduler_v2_maxjobs(self):
        conf = self.mgr.get_config('scheduler', 2)
        self.assertEqual(conf['yarn.scheduler.capacity.maximum-applications'],
                '100')

    def test_get_config_scheduler_v2_mincap(self):
        conf = self.mgr.get_config('scheduler', 2)
        self.assertEqual(conf['yarn.scheduler.capacity.root.tester.capacity'],
                '50')

    def test_get_config_scheduler_v2_users_tester(self):
        conf = self.mgr.get_config('scheduler', 2)
        self.assertEqual(conf['yarn.scheduler.capacity.root.tester.acl_submit_applications'],
                'bossman,trozamon')

    def test_get_config_scheduler_v2_admins_tester(self):
        conf = self.mgr.get_config('scheduler', 2)
        self.assertEqual(conf['yarn.scheduler.capacity.root.tester.acl_administer_queue'],
                'bossman')

    def test_get_config_scheduler_v2_users(self):
        conf = self.mgr.get_config('scheduler', 2)
        self.assertEqual(conf['yarn.scheduler.capacity.root.default.acl_submit_applications'],
                'root,trozamon')

    def test_get_config_scheduler_v2_admins(self):
        conf = self.mgr.get_config('scheduler', 2)
        self.assertEqual(conf['yarn.scheduler.capacity.root.default.acl_administer_queue'],
                'trozamon')


class IncludedDefaultConfigTest(unittest.TestCase):
    """ Test the default config in data/hadmin.yaml """

    def setUp(self):
        data = pkgutil.get_data('data', 'hadmin.yaml').decode('utf-8')
        self.mgr = Internal(yaml.load(data))

    def tearDown(self):
        self.mgr = None

    def test_data_scheduler_v1_init_garbage(self):
        conf = self.mgr.get_data('scheduler', 1)
        self.assertEqual(
                conf['scheduler']['mapred.capacity-scheduler.default-init-accept-jobs-factor'], 10)

    def test_data_scheduler_v1_user_limit_factor(self):
        conf = self.mgr.get_data('scheduler', 1)
        self.assertEqual(
                conf['scheduler']['mapred.capacity-scheduler.default-user-limit-factor'], 10)

    def test_data_scheduler_v1_supports_priority(self):
        conf = self.mgr.get_data('scheduler', 1)
        self.assertEqual(
                conf['scheduler']['mapred.capacity-scheduler.default-supports-priority'], 'true')

    def test_data_scheduler_v1_poll_interval(self):
        conf = self.mgr.get_data('scheduler', 1)
        self.assertEqual(
                conf['scheduler']['mapred.capacity-scheduler.init-poll-interval'], 5000)

    def test_data_scheduler_v1_worker_threads(self):
        conf = self.mgr.get_data('scheduler', 1)
        self.assertEqual(
                conf['scheduler']['mapred.capacity-scheduler.init-worker-threads'], 5)

    def test_data_scheduler_v1_maxjobs(self):
        conf = self.mgr.get_data('scheduler', 1)
        self.assertEqual(
                conf['scheduler']['maxjobs'], 10000)

    def test_data_scheduler_v1_maxtpq(self):
        conf = self.mgr.get_data('scheduler', 1)
        self.assertEqual(
                conf['scheduler']['maxtpq'], 200000)

    def test_data_scheduler_v1_maxtpu(self):
        conf = self.mgr.get_data('scheduler', 1)
        self.assertEqual(
                conf['scheduler']['maxtpu'], 100000)

    def test_data_scheduler_v1_ulim(self):
        conf = self.mgr.get_data('scheduler', 1)
        self.assertEqual(
                conf['scheduler']['ulim'], 25)

    def test_data_scheduler_v1_mincap(self):
        conf = self.mgr.get_data('scheduler', 1)
        self.assertEqual(
                conf['queues']['default']['mincap'], 5)

    def test_data_scheduler_v1_maxcap(self):
        conf = self.mgr.get_data('scheduler', 1)
        self.assertEqual(
                conf['queues']['default']['maxcap'], 10)

    def test_data_scheduler_v1_maxtpu(self):
        conf = self.mgr.get_data('scheduler', 1)
        self.assertEqual(
                conf['queues']['default']['maxtpu'], 100000)

    def test_config_scheduler_v1_default_init_garbage(self):
        conf = self.mgr.get_config('scheduler', 1)
        self.assertEqual(
                conf['mapred.capacity-scheduler.default-init-accept-jobs-factor'], '10')

    def test_config_scheduler_v1_ulim(self):
        conf = self.mgr.get_config('scheduler', 1)
        self.assertEqual(
                conf['mapred.capacity-scheduler.default-minimum-user-limit-percent'], '25')

    def test_config_scheduler_v1_support_priority(self):
        conf = self.mgr.get_config('scheduler', 1)
        self.assertEqual(
                conf['mapred.capacity-scheduler.default-supports-priority'], 'true')

    def test_config_scheduler_v1_poll_interval(self):
        conf = self.mgr.get_config('scheduler', 1)
        self.assertEqual(
                conf['mapred.capacity-scheduler.init-poll-interval'], '5000')

    def test_config_scheduler_v1_worker_threads(self):
        conf = self.mgr.get_config('scheduler', 1)
        self.assertEqual(
                conf['mapred.capacity-scheduler.init-worker-threads'], '5')

    def test_config_scheduler_v1_maxjobs(self):
        conf = self.mgr.get_config('scheduler', 1)
        self.assertEqual(
                conf['mapred.capacity-scheduler.maximum-system-jobs'], '10000')

    def test_config_scheduler_v1_maxtpq(self):
        conf = self.mgr.get_config('scheduler', 1)
        self.assertEqual(
                conf['mapred.capacity-scheduler.default-maximum-active-tasks-per-queue'], '200000')

    def test_config_scheduler_v1_maxtpu(self):
        conf = self.mgr.get_config('scheduler', 1)
        self.assertEqual(
                conf['mapred.capacity-scheduler.default-maximum-active-tasks-per-user'], '100000')

    def test_config_scheduler_v1_user_limit_factor(self):
        conf = self.mgr.get_config('scheduler', 1)
        self.assertEqual(
                conf['mapred.capacity-scheduler.default-user-limit-factor'], '10')

    def test_config_scheduler_v1_mincap(self):
        conf = self.mgr.get_config('scheduler', 1)
        self.assertEqual(
                conf['mapred.capacity-scheduler.queue.default.capacity'], '5')

    def test_config_scheduler_v1_maxcap(self):
        conf = self.mgr.get_config('scheduler', 1)
        self.assertEqual(
                conf['mapred.capacity-scheduler.queue.default.maximum-capacity'], '10')

    def test_config_scheduler_v1_maxtpu_users(self):
        conf = self.mgr.get_config('scheduler', 1)
        self.assertEqual(
                conf['mapred.capacity-scheduler.queue.default.maximum-initialized-active-tasks-per-user'], '100000')

    def test_data_queues_v1(self):
        conf = self.mgr.get_data('queues', 1)
        self.assertEqual(conf['queues']['default']['admins'], 'root')
        self.assertEqual(conf['queues']['default']['users'], 'root')

    def test_config_queues_v1(self):
        conf = self.mgr.get_config('queues', 1)
        self.assertEqual(conf['mapred.queue.default.acl-submit-job'], 'root')
        self.assertEqual(conf['mapred.queue.default.acl-administer-jobs'], 'root')

    def test_data_queues_v2(self):
        conf = self.mgr.get_data('queues', 2)
        self.assertEqual(len(conf), 2)
        self.assertEqual(len(conf['queues']), 1)
        self.assertEqual(len(conf['scheduler']), 0)

    def test_config_queues_v2(self):
        conf = self.mgr.get_config('queues', 2)
        self.assertEqual(len(conf.conf), 0)

    def test_data_scheduler_v2_admins(self):
        conf = self.mgr.get_data('scheduler', 2)
        self.assertEqual(conf['queues']['default']['admins'], 'root')

    def test_data_scheduler_v2_mincap(self):
        conf = self.mgr.get_data('scheduler', 2)
        self.assertEqual(conf['queues']['default']['mincap'], 5)

    def test_data_scheduler_v2_maxcap(self):
        conf = self.mgr.get_data('scheduler', 2)
        self.assertEqual(conf['queues']['default']['maxcap'], 10)

    def test_data_scheduler_v2_state(self):
        conf = self.mgr.get_data('scheduler', 2)
        self.assertEqual(conf['queues']['default']['state'], 'RUNNING')

    def test_data_scheduler_v2_users(self):
        conf = self.mgr.get_data('scheduler', 2)
        self.assertEqual(conf['queues']['default']['users'], 'root')

    def test_data_scheduler_v2_maxjobs(self):
        conf = self.mgr.get_data('scheduler', 2)
        self.assertEqual(conf['scheduler']['maxjobs'], 10000)

    def test_data_scheduler_v2_maximum_am_resource_percent(self):
        conf = self.mgr.get_data('scheduler', 2)
        self.assertEqual(
                conf['scheduler']['yarn.scheduler.capacity.maximum-am-resource-percent'],
                0.1)

    def test_data_scheduler_v2_node_locality_delay(self):
        conf = self.mgr.get_data('scheduler', 2)
        self.assertEqual(
                conf['scheduler']['yarn.scheduler.capacity.node-locality-delay'],
                -1)

    def test_data_scheduler_v2_resource_calculator(self):
        conf = self.mgr.get_data('scheduler', 2)
        self.assertEqual(
                conf['scheduler']['yarn.scheduler.capacity.resource-calculator'],
                'org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator')

    def test_config_scheduler_v2_admins(self):
        conf = self.mgr.get_config('scheduler', 2)
        self.assertEqual(
                conf['yarn.scheduler.capacity.root.default.acl_administer_queue'], 'root')

    def test_config_scheduler_v2_mincap(self):
        conf = self.mgr.get_config('scheduler', 2)
        self.assertEqual(
                conf['yarn.scheduler.capacity.root.default.capacity'], '5')

    def test_config_scheduler_v2_maxcap(self):
        conf = self.mgr.get_config('scheduler', 2)
        self.assertEqual(
                conf['yarn.scheduler.capacity.root.default.maximum-capacity'], '10')

    def test_config_scheduler_v2_state(self):
        conf = self.mgr.get_config('scheduler', 2)
        self.assertEqual(
                conf['yarn.scheduler.capacity.root.default.state'], 'RUNNING')

    def test_config_scheduler_v2_ulim(self):
        conf = self.mgr.get_config('scheduler', 2)
        self.assertEqual(
                conf['yarn.scheduler.capacity.root.default.user-limit-factor'],
                '100')

    def test_config_scheduler_v2_users(self):
        conf = self.mgr.get_config('scheduler', 2)
        self.assertEqual(
                conf['yarn.scheduler.capacity.root.default.acl_submit_applications'], 'root')

    def test_config_scheduler_v2_maxjobs(self):
        conf = self.mgr.get_config('scheduler', 2)
        self.assertEqual(conf['yarn.scheduler.capacity.maximum-applications'],
                '10000')

    def test_config_scheduler_v2_maximum_am_resource_percent(self):
        conf = self.mgr.get_config('scheduler', 2)
        self.assertEqual(
                conf['yarn.scheduler.capacity.maximum-am-resource-percent'],
                '0.1')

    def test_config_scheduler_v2_node_locality_delay(self):
        conf = self.mgr.get_config('scheduler', 2)
        self.assertEqual(
                conf['yarn.scheduler.capacity.node-locality-delay'],
                '-1')

    def test_config_scheduler_v2_resource_calculator(self):
        conf = self.mgr.get_config('scheduler', 2)
        self.assertEqual(
                conf['yarn.scheduler.capacity.resource-calculator'],
                'org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator')

class TypeCheckerTest(unittest.TestCase):

    def setUp(self):
        self.chkr = TypeChecker()

    def tearDown(self):
        self.chkr = None

    def test_ulim_int(self):
        self.assertTrue(self.chkr.check('ulim', 1))

    def test_ulim_float(self):
        self.assertTrue(self.chkr.check('ulim', 1.1))

    def test_ulim_str(self):
        self.assertFalse(self.chkr.check('ulim', 'hey'))

    def test_ulim_list(self):
        self.assertFalse(self.chkr.check('ulim', [1]))

    def test_admins_csv(self):
        self.assertTrue(self.chkr.check('admins', 'alec,bob'))

    def test_admins_str(self):
        self.assertTrue(self.chkr.check('admins', 'alec'))

    def test_admins_num(self):
        self.assertFalse(self.chkr.check('admins', 1))

    def test_admins_list(self):
        self.assertFalse(self.chkr.check('admins', ['alec', 'bob']))

    def test_mincap_int(self):
        self.assertTrue(self.chkr.check('mincap', 1))

    def test_mincap_float(self):
        self.assertTrue(self.chkr.check('mincap', 1.1))

    def test_mincap_str(self):
        self.assertFalse(self.chkr.check('mincap', 'hey'))

    def test_mincap_list(self):
        self.assertFalse(self.chkr.check('mincap', [1]))

    def test_maxcap_int(self):
        self.assertTrue(self.chkr.check('maxcap', 1))

    def test_maxcap_float(self):
        self.assertTrue(self.chkr.check('maxcap', 1.1))

    def test_maxcap_str(self):
        self.assertFalse(self.chkr.check('maxcap', 'hey'))

    def test_maxcap_list(self):
        self.assertFalse(self.chkr.check('maxcap', [1]))

    def test_running_str(self):
        self.assertTrue(self.chkr.check('running', 'yessir'))

    def test_running_csv(self):
        self.assertFalse(self.chkr.check('running', 'yes,sir'))

    def test_running_num(self):
        self.assertFalse(self.chkr.check('running', 1))

    def test_running_list(self):
        self.assertFalse(self.chkr.check('running', ['yes']))

    def test_maxjobs_int(self):
        self.assertTrue(self.chkr.check('maxjobs', 1))

    def test_maxjobs_float(self):
        self.assertTrue(self.chkr.check('maxjobs', 1.1))

    def test_maxjobs_str(self):
        self.assertFalse(self.chkr.check('maxjobs', 'hye'))

    def test_maxjobs_list(self):
        self.assertFalse(self.chkr.check('maxjobs', [1]))

    def test_maxtpu_int(self):
        self.assertTrue(self.chkr.check('maxtpu', 1))

    def test_maxtpu_float(self):
        self.assertTrue(self.chkr.check('maxtpu', 1.1))

    def test_maxtpu_str(self):
        self.assertFalse(self.chkr.check('maxtpu', 'hye'))

    def test_maxtpu_list(self):
        self.assertFalse(self.chkr.check('maxtpu', [1]))

    def test_maxtpq_int(self):
        self.assertTrue(self.chkr.check('maxtpq', 1))

    def test_maxtpq_float(self):
        self.assertTrue(self.chkr.check('maxtpq', 1.1))

    def test_maxtpq_str(self):
        self.assertFalse(self.chkr.check('maxtpq', 'hye'))

    def test_maxtpq_list(self):
        self.assertFalse(self.chkr.check('maxtpq', [1]))

    def test_num_int(self):
        self.assertTrue(self.chkr._check_num(1))

    def test_num_int_neg(self):
        self.assertTrue(self.chkr._check_num(-1))

    def test_num_float(self):
        self.assertTrue(self.chkr._check_num(1.1))

    def test_num_float_neg(self):
        self.assertTrue(self.chkr._check_num(-1.1))

    def test_num_str(self):
        self.assertFalse(self.chkr._check_num('hey'))

    def test_num_list(self):
        self.assertFalse(self.chkr._check_num([1]))

    def test_num_tuple(self):
        self.assertFalse(self.chkr._check_num((1,2)))

    def test_str(self):
        self.assertTrue(self.chkr._check_str('hey'))
        
    def test_str_csv(self):
        self.assertFalse(self.chkr._check_str('hey,ah'))

    def test_csv(self):
        self.assertTrue(self.chkr._check_csv('hey'))
        
    def test_csv_csv(self):
        self.assertTrue(self.chkr._check_csv('hey,ah'))
