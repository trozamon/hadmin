""" Auto generation of users, queues, etc. """

import re
import os
from hconfig import Config, ConfigValue

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
        tmps = []
        tmp = ConfigValue()
        is_queue_file = False

        if re.search("\.users", f):
            is_queue_file = True
            tmp.key = "mapred.queue." + queue_name + ".acl-submit-job"
            tmp.set_is_final(True)
            user_list = ""
            for line in open(filename):
                line = line.rstrip("\n")
                if len(user_list) > 0:
                    user_list = user_list + ","
                user_list = user_list + line
            tmp.value = user_list
            tmps.append(tmp)
        elif re.search("\.admins", f):
            is_queue_file = True
            tmp.key = "mapred.queue." + queue_name + ".acl-administer-jobs"
            tmp.set_is_final(True)
            admin_list = ""
            for line in open(filename):
                line = line.rstrip("\n")
                if len(admin_list) > 0:
                    admin_list = admin_list + ","
                admin_list = admin_list + line
            tmp.value = admin_list
            tmps.append(tmp)
        elif re.search("\.settings", f):
            is_queue_file = True
            conf = Config.from_yaml(filename)
            for conf_val in conf.configs:
                tmps.append(conf_val)

        if is_queue_file:
            try:
                arr[queue_name]
            except KeyError:
                arr[queue_name] = Config([])
            for conf_val in tmps:
                arr[queue_name].configs.append(conf_val)

    return arr

def create_configs(directory):
    """ Creates all the config files. Adds queue and user info to
    capacity-scheduler.xml, mapred-queue-acls.xml, and mapred-site.xml. """

    configs = {}
    configs['capacity-scheduler.xml'] = Config.from_yaml(directory + "/capacity-scheduler.yaml")
    configs['core-site.xml'] = Config.from_yaml(directory + "/core-site.yaml")
    configs['hadoop-policy.xml'] = Config.from_yaml(directory + "/hadoop-policy.yaml")
    configs['hdfs-site.xml'] = Config.from_yaml(directory + "/hdfs-site.yaml")
    configs['mapred-queue-acls.xml'] = Config.from_yaml(directory + "/mapred-queue-acls.yaml")
    configs['mapred-site.xml'] = Config.from_yaml(directory + "/mapred-site.yaml")

    queues = create_queues(directory)

    queue_list = ""
    for queue in queues:
        if len(queue_list) > 0:
            queue_list = queue_list + ","
        queue_list = queue_list + queue
        for conf in queues[queue].configs:
            if conf.key == "mapred.queue." + queue + ".acl-submit-job":
                users = conf
            elif conf.key == "mapred.queue." + queue + ".acl-administer-jobs":
                admins = conf
            elif conf.key == "mapred.capacity-scheduler.queue." + queue + ".capacity":
                cap = conf
            elif conf.key == "mapred.capacity-scheduler.queue." + queue + ".maximum-capacity":
                maxcap = conf
            elif conf.key == "mapred.capacity-scheduler.queue." + queue + ".maximum-initialized-active-tasks-per-user":
                init_tasks = conf
        configs['capacity-scheduler.xml'].configs.append(
    
if __name__ == "__main__":
    import sys

    if (len(sys.argv) != 2):
        print("Usage: " + __file__ + " <directory>")
        exit(1)

    fname = sys.argv[1]
    tmp = create_queues(fname)
    for conf in tmp:
        print(conf)
        print(tmp[conf])
        print()
