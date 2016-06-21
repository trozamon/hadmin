"""
HDFS management. Manage the filesystem, get namenode stats, etc.
"""


import subprocess


class NameNode:
    """
    Deal with the namenode. Manage HDFS. etc.
    """

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
