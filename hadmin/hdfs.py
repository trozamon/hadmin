"""
HDFS-related library functionality
----------------------------------

HDFS management. Manage the filesystem, get namenode stats, etc.
"""


import subprocess
from hadmin.util import run_or_warn


def perms_pretty_to_octal_str(raw):
    owner_perms = raw[1:4]
    group_perms = raw[4:7]
    world_perms = raw[7:10]
    pretty = ['0', '0', '0', '0']

    def triplet_to_octal(triplet):
        res = 0

        if triplet[0] == 'r':
            res += 4

        if triplet[1] == 'w':
            res += 2

        if triplet[2] == 'x' or triplet[2] == 't':
            res += 1

        return str(res)

    pretty[1] = triplet_to_octal(owner_perms)
    pretty[2] = triplet_to_octal(group_perms)
    pretty[3] = triplet_to_octal(world_perms)

    if world_perms[-1] == 't':
        pretty[0] = '1'

    return ''.join(pretty)


class Directory:

    def __init__(self, path, owner, group, perms):
        self.path = path
        self.owner = owner
        self.group = group
        self.perms = perms

    @classmethod
    def from_hdfs_ls_str(cls, s):
        arr = s.split()

        perms_raw = arr[0]
        owner = arr[2]
        group = arr[3]
        path = arr[7]

        return Directory(path, owner, group,
                         perms_pretty_to_octal_str(perms_raw))

    @classmethod
    def from_hdfs(cls, path):
        listing = run_or_warn('hdfs dfs -ls -d ' + path,
                              'failed to list directory')
        if listing.status != 0:
            raise IOError(path + " does not exist")

        return cls.from_hdfs_ls_str(listing.output)

    def write(self):
        if run_or_warn('hdfs dfs -test -d ' + self.path).status == 1:
            run_or_warn('hdfs dfs -mkdir ' + self.path,
                        'failed to create directory ' + self.path)

        run_or_warn('hdfs dfs -chown ' + self.owner + ':' + self.group + ' ' +
                    self.path, 'failed to chown directory')

        run_or_warn('hdfs dfs -chmod ' + self.perms + ' ' + self.path,
                    'failed to chmod directory')

    def __eq__(self, other):
        paths = self.path == other.path
        owners = self.owner == other.owner
        groups = self.group == other.group
        perms = self.perms == other.perms

        return paths and owners and groups and perms

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return ' '.join([self.perms, self.owner, self.group, self.path])


class NameNode:
    """
    Deal with the namenode. Manage HDFS. etc.
    """

    FHS_DIRS = [
            Directory('/', 'hdfs', 'hadoop', '0755'),
            Directory('/tmp', 'yarn', 'hadoop', '1777'),
            Directory('/user', 'hdfs', 'hadoop', '0755')
            ]

    def __init__(self, addr):
        self.addr = addr

    def add_home_for(self, user):
        """
        Adds user directories. This calls other binaries using subprocess.
        """

        ret = subprocess.call('which hdfs', shell=True)
        if ret != 0:
            print('You do not have the hdfs binary on your system')
            print('You will have to manually run:')
            print('\thdfs dfs -mkdir /user/' + user)
            print('\thdfs dfs -chown ' + user + ' /user/' + user)
            return ret

        ret = subprocess.call('hdfs dfs -mkdir /user/' + user, shell=True)
        if ret != 0:
            print('Creating directory /user/' + user + ' failed')
            return ret
        print('Created home directory for ' + user)

        ret = subprocess.call('hdfs dfs -chown ' + user + ' /user/' + user,
                              shell=True)
        if ret != 0:
            print('Chowning the above directory failed')
            return ret

        return 0
