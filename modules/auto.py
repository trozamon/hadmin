""" Auto generation of users, queues, etc. """

import re
import os
from hconfig import Config

def create_queues(directory):
    """ Reads all the .users, .admins, and .settings in a given directory.
    Creates an two-dimension array, with the first field as the name of the
    queue, and the second field as 'users', 'admins', or 'settings'. 'users'
    and 'admins' return a CSV list, and 'settings' returns an array of
    ConfigValues. """
    arr = {}
    for f in os.listdir(directory):
        filename = directory + "/" + f
        queue_name = f.split(".")[0]
        if re.search("\.users", f):
            user_list = ""
            for line in open(filename):
                line = line.rstrip("\n")
                if len(user_list) > 0:
                    user_list = user_list + ","
                user_list = user_list + line
            try:
                arr[queue_name]['users'] = user_list
            except KeyError:
                arr[queue_name] = {}
                arr[queue_name]['users'] = user_list
        if re.search("\.admins", f):
            admin_list = ""
            for line in open(filename):
                line = line.rstrip("\n")
                if len(admin_list) > 0:
                    admin_list = admin_list + ","
                admin_list = admin_list + line
            try:
                arr[queue_name]['admins'] = admin_list
            except KeyError:
                arr[queue_name] = {}
                arr[queue_name]['admins'] = admin_list
        if re.search("\.settings", f):
            conf = Config.from_yaml(filename)
            try:
                arr[queue_name]['settings'] = conf
            except KeyError:
                arr[queue_name] = {}
                arr[queue_name]['settings'] = conf
    return arr

if __name__ == "__main__":
    import sys

    if (len(sys.argv) != 2):
        print("Usage: " + __file__ + " <xml-file>")
        exit(1)

    fname = sys.argv[1]
    tmp = create_queues(fname)
    print(tmp)
