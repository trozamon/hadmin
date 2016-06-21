from hadmin.util import HXML
from hadmin.yarn import Queue
from unittest2 import TestCase


class HXMLTest(TestCase):

    def setUp(self):
        self.hxml = HXML.from_file('data/capacity-scheduler.xml')

    def testHXMLGetter(self):
        self.assertEqual('trozamon,root',
                         self.hxml[Queue.fqn_users('root.a')])

    def testHXMLGetterNonExistent(self):
        with self.assertRaises(KeyError):
            self.hxml['roar']

    def testHXMLSetter(self):
        self.hxml[Queue.fqn_users('root.a')] = 'hehe'
        self.assertEqual('hehe',
                         self.hxml[Queue.fqn_users('root.a')])

    def testHXMLSetterNonExistent(self):
        self.hxml['hehe'] = 'hey'
        self.assertEqual(self.hxml['hehe'], 'hey')
