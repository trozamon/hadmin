import unittest
import copy
import re
from hadmin.config import Config, Internal


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
                    'cap': '50',
                    'max-cap': '60',
                    'users': 'bossman,trozamon'
                    },
                'default': {
                    'admins': 'trozamon',
                    'cap': '5',
                    'max-cap': '10',
                    'users': 'root,trozamon'
                    }
                },
            'scheduler': {
                'max-jobs': '100',
                'user-limit-factor': '10'
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
        self.assertEqual(self.mgr.conf['queues']['test']['cap'], 0)
        self.assertEqual(self.mgr.conf['queues']['test']['max-cap'], 0)
        self.assertEqual(self.mgr.conf['queues']['test']['max-tpu'], 0)

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
        self.assertEqual(self.mgr.conf['queues']['default']['cap'], 10)

    def test_set_queue_max_cap(self):
        self.mgr.set_queue_max_cap('default', 10)
        self.assertEqual(self.mgr.conf['queues']['default']['max-cap'], 10)

    def test_set_queue_max_init_tpu(self):
        self.mgr.set_queue_max_init_tpu('default', 10)
        self.assertEqual(self.mgr.conf['queues']['default']['max-tpu'], 10)

    def test_get_data_queues_v1(self):
        conf = self.mgr.get_data('queues', 1)
        self.assertEqual(conf['queues']['tester']['admins'], 'bossman')
        self.assertEqual(conf['queues']['tester']['users'], 'bossman,trozamon')
        self.assertEqual(conf['queues']['default']['admins'], 'trozamon')
        self.assertEqual(conf['queues']['default']['users'], 'root,trozamon')

    def test_get_config_queues_v1(self):
        conf = self.mgr.get_config('queues', 1)
        self.assertEqual(conf['mapred.queue.tester.acl-submit-job'],
                'bossman,trozamon')
        self.assertEqual(conf['mapred.queue.tester.acl-administer-jobs'],
                'bossman')
        self.assertEqual(conf['mapred.queue.default.acl-submit-job'],
                'root,trozamon')
        self.assertEqual(conf['mapred.queue.default.acl-administer-jobs'],
                'trozamon')

    def test_get_data_queues_v2(self):
        conf = self.mgr.get_data('queues', 2)
        self.assertEqual(conf['queues']['tester']['admins'], 'bossman')
        self.assertEqual(conf['queues']['tester']['users'], 'bossman,trozamon')
        self.assertEqual(conf['queues']['default']['admins'], 'trozamon')
        self.assertEqual(conf['queues']['default']['users'], 'root,trozamon')

    def test_get_config_queues_v2(self):
        conf = self.mgr.get_config('queues', 2)
        self.assertEqual(conf['yarn.scheduler.capacity.root.tester.acl_submit_applications'],
                'bossman,trozamon')
        self.assertEqual(conf['yarn.scheduler.capacity.root.tester.acl_administer_queue'],
                'bossman')
        self.assertEqual(conf['yarn.scheduler.capacity.root.default.acl_submit_applications'],
                'root,trozamon')
        self.assertEqual(conf['yarn.scheduler.capacity.root.default.acl_administer_queue'],
                'trozamon')

    def test_get_data_scheduler_v1(self):
        conf = self.mgr.get_data('scheduler', 1)
        self.assertEqual(conf['queues']['tester']['cap'], '50')
        self.assertEqual(conf['queues']['tester']['max-cap'], '60')
        self.assertEqual(conf['queues']['default']['cap'], '5')
        self.assertEqual(conf['queues']['default']['max-cap'], '10')
        self.assertEqual(conf['scheduler']['max-jobs'], '100')
        self.assertEqual(conf['scheduler']['user-limit-factor'], '10')

    def test_get_config_scheduler_v1(self):
        conf = self.mgr.get_config('scheduler', 1)
        self.assertEqual(conf['mapred.capacity-scheduler.maximum-system-jobs'],
                '100')
        self.assertEqual(conf['mapred.capacity-scheduler.queue.tester.capacity'],
                '50')

    def test_get_data_scheduler_v2(self):
        conf = self.mgr.get_data('scheduler', 2)
        self.assertEqual(conf['queues']['tester']['cap'], '50')
        self.assertEqual(conf['queues']['tester']['max-cap'], '60')
        self.assertEqual(conf['queues']['default']['cap'], '5')
        self.assertEqual(conf['queues']['default']['max-cap'], '10')
        self.assertEqual(conf['scheduler']['max-jobs'], '100')
        self.assertEqual(conf['scheduler']['user-limit-factor'], '10')

    def test_get_config_scheduler_v2(self):
        conf = self.mgr.get_config('scheduler', 2)
        self.assertEqual(conf['yarn.scheduler.capacity.maximum-applications'],
                '100')
        self.assertEqual(conf['yarn.scheduler.capacity.root.tester.capacity'],
                '50')
