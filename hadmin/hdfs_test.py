from hadmin.hdfs import Directory
from unittest2 import TestCase


class DirectoryTest(TestCase):

    def setUp(self):
        raw = 'drwxr-xr-x   - alec supergroup          0 1969-12-31 19:00 /'
        self.d = Directory.from_hdfs_ls_str(raw)
        self.dclone = Directory(
                path='/',
                owner='alec',
                group='supergroup',
                perms='0755'
                )

    def testEquality(self):
        self.assertEqual(self.d, self.dclone)

    def testInequality(self):
        self.dclone.path = '/heyo'
        self.assertNotEqual(self.d, self.dclone)

    def testFromHDFSLsStrPerms(self):
        self.assertEqual('0755', self.d.perms)

    def testFromHDFSLsStrOwner(self):
        self.assertEqual('alec', self.d.owner)

    def testFromHDFSLsStrGroup(self):
        self.assertEqual('supergroup', self.d.group)

    def testFromHDFSLsStrPath(self):
        self.assertEqual('/', self.d.path)
