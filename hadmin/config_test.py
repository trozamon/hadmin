import unittest
import copy
from hadmin.config import *

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

class MapperTest(unittest.TestCase):

    def setUp(self):
        self.mapper = Mapper('-', '.')
        self.mapper['hey'] = 'yo.-.wazzup'

    def tearDown(self):
        self.mapper = None

    def test_map_empty(self):
        with self.assertRaises(KeyError):
            self.mapper['test']

    def test_map_forward(self):
        self.assertEqual(self.mapper['hey'], 'yo.-.wazzup')

    def test_map_reverse(self):
        self.assertEqual(self.mapper['yo.-.wazzup'], 'hey')

    def test_map_subs_forward(self):
        self.assertEqual(self.mapper['hey', 'alec'], 'yo.alec.wazzup')

    def test_map_subs_reverse(self):
        self.assertEqual(self.mapper['yo.alec.wazzup'], ('alec', 'hey'))

    def test_find_bare_key(self):
        self.assertEqual(self.mapper.find_bare_key('yo.alec.wazzup'), 'yo.-.wazzup')

class HadminManagerTest(unittest.TestCase):

    def setUp(self):
        self.data = {
                'queues': {
                    'default': {
                        'admins': 'trozamon',
                        'capacity': 5,
                        'max-cap': 10,
                        'max-tpu': 100000,
                        'users': 'trozamon,root'
                        }
                    },
                'max-sys-jobs': '100',
                'supports-priority': 'true',
                'min-user-lim-perc': '25',
                'user-lim-factor': '10',
                'max-tpq': '200000',
                'max-tpu': '100000',
                'accept-jobs-factor': '10',
                'poll-interval': '5000',
                'worker-threads': '5'
                }
        self.mgr = Hadmin(self.data)

    def tearDown(self):
        self.mgr = None
        self.data = None

    def test_add_admin(self):
        self.mgr.add_admin('fluffy', 'default')
        self.assertTrue(self.mgr.conf['queues']['default']['admins'] == \
                'fluffy,trozamon')

    def test_add_queue(self):
        self.mgr.add_queue('test', 'trozamon')
        self.assertTrue(self.mgr.conf['queues']['test']['admins'] == 'trozamon')
        self.assertTrue(self.mgr.conf['queues']['test']['users'] == 'trozamon')
        self.assertTrue(self.mgr.conf['queues']['test']['capacity'] == 0)
        self.assertTrue(self.mgr.conf['queues']['test']['max-cap'] == 0)
        self.assertTrue(self.mgr.conf['queues']['test']['max-tpu'] == 0)

    def test_add_user(self):
        self.mgr.add_user('fluffy', 'default')
        self.assertTrue(self.mgr.conf['queues']['default']['users'] == \
                'fluffy,root,trozamon')

    def test_check_queue(self):
        self.mgr.check_queue('default')
        with self.assertRaises(KeyError):
            self.mgr.check_queue('test')

    def test_del_admin(self):
        self.mgr.add_admin('fluffy', 'default')
        self.mgr.del_admin('trozamon', 'default')
        self.assertTrue(self.mgr.conf['queues']['default']['admins'] == \
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
        self.assertTrue(self.mgr.conf['queues']['default']['capacity'] == 10)

    def test_set_queue_max_cap(self):
        self.mgr.set_queue_max_cap('default', 10)
        self.assertTrue(self.mgr.conf['queues']['default']['max-cap'] == 10)

    def test_set_queue_max_init_tpu(self):
        self.mgr.set_queue_max_init_tpu('default', 10)
        self.assertTrue(self.mgr.conf['queues']['default']['max-tpu'] == 10)
