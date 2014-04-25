import unittest
from hadmin.config import Config

class ConfigTest(unittest.TestCase):

    def setUp(self):
        self.conf = Config()

    def tearDown(self):
        self.conf = None

    def test_add_key(self):
        self.assertTrue(len(self.conf.conf.keys()) == 0)
        self.conf.add_key('test')
        self.assertTrue(len(self.conf.conf.keys()) == 1)
        self.assertTrue('test' in self.conf.conf.keys())

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
