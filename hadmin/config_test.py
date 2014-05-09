import unittest
from hadmin.config import *

class ConfigTest(unittest.TestCase):

    def setUp(self):
        self.conf = Config()

    def tearDown(self):
        self.conf = None

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
