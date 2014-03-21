""" Auto generation of users, queues, etc. """

import re
import os
from hadmin.hconfig import Config, ConfigValue
from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

def create_queues(directory):
    """ Reads all the .users, .admins, and .settings in a given directory.
    Creates an two-dimension array, with the first field as the name of the
    queue, and the second field as 'users', 'admins', or 'settings'. 'users'
    and 'admins' return a CSV list, and 'settings' returns an array of
    ConfigValues. """
    arr = {}

    f = open(directory + "/hadmin-queues.yaml", "r")
    data = load(f, Loader=Loader)
    for elem in data:
        arr[elem] = []
        for key in data[elem]:
            tmp = ConfigValue()
            if key == "users":
                tmp.key = "mapred.queue." + elem + ".acl-submit-job"
            elif key == "admins":
                tmp.key = "mapred.queue." + elem + ".acl-administer-jobs"
            elif key == "capacity":
                tmp.key = "mapred.capacity-scheduler.queue." + elem + ".capacity"
            elif key == "maximum-capacity":
                tmp.key = "mapred.capacity-scheduler.queue." + elem + ".maximum-capacity"
            elif key == "maximum-initialized-active-tasks-per-user":
                tmp.key = "mapred.capacity-scheduler.queue." + elem + ".maximum-initialized-active-tasks-per-user"
            tmp.is_final = True
            tmp.value = data[elem][key]
            arr[elem].append(tmp)

    return arr

def create_configs(directory="."):
    """ Creates all the config files. Adds queue and user info to
    capacity-scheduler.xml, mapred-queue-acls.xml, and mapred-site.xml. """

    configs = {}
    configs['capacity-scheduler.xml'] = Config.from_yaml(directory + "/capacity-scheduler.yaml")
    configs['core-site.xml'] = Config.from_yaml(directory + "/core-site.yaml")
    configs['hadoop-policy.xml'] = Config.from_yaml(directory + "/hadoop-policy.yaml")
    configs['hdfs-site.xml'] = Config.from_yaml(directory + "/hdfs-site.yaml")
    configs['mapred-site.xml'] = Config.from_yaml(directory + "/mapred-site.yaml")
    configs['mapred-queue-acls.xml'] = Config(None)

    queues = create_queues(directory)

    queue_list = ""
    for queue in queues:
        if len(queue_list) > 0:
            queue_list = queue_list + ","
        queue_list = queue_list + queue
        for conf in queues[queue]:
            if conf.key == "mapred.queue." + queue + ".acl-submit-job":
                configs['mapred-queue-acls.xml'].configs.append(conf)
            elif conf.key == "mapred.queue." + queue + ".acl-administer-jobs":
                configs['mapred-queue-acls.xml'].configs.append(conf)
            elif conf.key == "mapred.capacity-scheduler.queue." + queue + ".capacity":
                configs['capacity-scheduler.xml'].configs.append(conf)
            elif conf.key == "mapred.capacity-scheduler.queue." + queue + ".maximum-capacity":
                configs['capacity-scheduler.xml'].configs.append(conf)
            elif conf.key == "mapred.capacity-scheduler.queue." + queue + ".maximum-initialized-active-tasks-per-user":
                configs['capacity-scheduler.xml'].configs.append(conf)
    tmp = ConfigValue()
    tmp.key = "mapred.queue.names"
    tmp.value = queue_list
    configs['mapred-site.xml'].configs.append(tmp)
    return configs
    
if __name__ == "__main__":
    import sys

    if (len(sys.argv) != 2):
        print("Usage: " + __file__ + " <directory>")
        exit(1)

    fname = sys.argv[1]
    tmp = create_configs(fname)
    for conf in tmp:
        print(conf)
        print(tmp[conf])
        print(tmp[conf].to_xml())
        print()
