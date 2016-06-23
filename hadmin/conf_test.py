from hadmin.conf import QueueGenerator
from unittest2 import TestCase


class QueueGeneratorTest(TestCase):

    def setUp(self):
        self.root = QueueGenerator.load_dir('data/queues').generate()
        self.prod = self.root.subqueue('prod')
        self.dev = self.root.subqueue('dev')
        self.dev1 = self.dev.subqueue('product1')
        self.dev2 = self.dev.subqueue('product2')

    def testRootCapacity(self):
        self.assertEqual(100.0, self.root.cap_min)

    def testRootUserLimitFactor(self):
        self.assertEqual(1.0, self.root.user_limit_factor)

    def testRootUsers(self):
        self.assertEqual(['*'], self.root.users)

    def testRootAdmins(self):
        self.assertEqual(['*'], self.root.admins)

    def testProdAdmins(self):
        self.assertEqual(['alec'], self.prod.admins)

    def testProdCapacity(self):
        self.assertEqual(90.0, self.prod.cap_min)

    def testProdUsers(self):
        self.assertEqual(['alec', 'trozamon'], self.prod.users)

    def testProdUserLimitFactor(self):
        self.assertAlmostEqual(50.0 / 90.0, self.prod.user_limit_factor)

    def testProdRunning(self):
        self.assertEqual(True, self.prod.running)

    def testProdMaxCapacity(self):
        self.assertEqual(100.0, self.prod.cap_max)

    def testDevCapacity(self):
        self.assertEqual(10.0, self.dev.cap_min)

    def testDevMaxCapacity(self):
        self.assertEqual(100.0, self.dev.cap_max)

    def testDevAdmins(self):
        self.assertEqual(['alec', 'trozamon'], self.dev.admins)

    def testDevUsers(self):
        self.assertEqual(['alec', 'trozamon'], self.dev.users)

    def testDevUserLimitFactor(self):
        self.assertEqual(10.0, self.dev.user_limit_factor)

    def testDevRunning(self):
        self.assertEqual(True, self.dev.running)

    def testDev1Capacity(self):
        self.assertEqual(50.0, self.dev1.cap_min)

    def testDev1MaxCapacity(self):
        self.assertEqual(100.0, self.dev1.cap_max)

    def testDev1Admins(self):
        self.assertEqual(['alec'], self.dev1.admins)

    def testDev1Users(self):
        self.assertEqual(['alec'], self.dev1.users)

    def testDev1UserLimitFactor(self):
        self.assertEqual(1.0, self.dev1.user_limit_factor)

    def testDev1Running(self):
        self.assertEqual(False, self.dev1.running)

    def testDev2Capacity(self):
        self.assertEqual(50.0, self.dev2.cap_min)

    def testDev2MaxCapacity(self):
        self.assertEqual(100.0, self.dev2.cap_max)

    def testDev2Admins(self):
        self.assertEqual(['trozamon'], self.dev2.admins)

    def testDev2Users(self):
        self.assertEqual(['trozamon'], self.dev2.users)

    def testDev2UserLimitFactor(self):
        self.assertEqual(1.0, self.dev2.user_limit_factor)

    def testDev2Running(self):
        self.assertEqual(True, self.dev2.running)
