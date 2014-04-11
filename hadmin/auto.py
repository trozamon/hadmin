""" Auto generation of users, queues, etc. """

# TODO Holy crap just rewrite this entire thing. hadmin.Config has changed
# quite a bit and this whole thing is completely screwed.

import re
import os
from hadmin.config import Config
from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

hadmin_key_rep = "____"
hadmin_key_map = dict()
hadmin_key_map['users'] = "mapred.queue." + hadmin_key_rep + ".acl-submit-job"
hadmin_key_map["admins"] = \
        "mapred.queue." + hadmin_key_rep + ".acl-administer-jobs"
hadmin_key_map["capacity"] = \
        "mapred.capacity-scheduler.queue." + hadmin_key_rep + ".capacity"
hadmin_key_map["max-cap"] = \
        "mapred.capacity-scheduler.queue." + hadmin_key_rep + \
        ".maximum-capacity"
hadmin_key_map["max-init-tpu"] = \
        "mapred.capacity-scheduler.queue." + hadmin_key_rep + \
        ".maximum-initialized-active-tasks-per-user"

def get_file_map(queue):
    if len(queue) == 0:
        raise ValueError("Queue must be a valid queue")
    file_map = dict()
    for key in hadmin_key_map:
        match = re.sub(hadmin_key_rep, queue, hadmin_key_map[key])
        if not match:
            raise KeyError("Improperly mapped hadmin key: " + key)
        if key in ["users", "admins"]:
            file_map[match] = "mapred-queue-acls.xml"
        elif key in ["capacity", "max-cap", "max-init-tpu"]:
            file_map[match] = "capacity-scheduler.xml"
    return file_map

# TODO Actually comment this mofo
def create_queues(directory):

    arr = dict()

    f = open(directory + "/hadmin-queues.yaml", "r")
    data = load(f, Loader=Loader)
    for queue in data:
        arr[queue] = dict()
        for opt in data[queue]:
            key = hadmin_key_map[opt]
            match = re.sub(hadmin_key_rep, queue, key)
            if not match:
                raise KeyError("Improperly mapped hadmin key: " + opt + " in queue " + queue)
            arr[queue][match] = str(data[queue][opt])

    return arr

def create_configs(directory="."):
    """ Creates all the config files. Adds queue and user info to
    capacity-scheduler.xml, mapred-queue-acls.xml, and mapred-site.xml. """

    configs = dict()
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
        file_map = get_file_map(queue)
        for conf in queues[queue]:
            configs[file_map[conf]].configs[conf] = queues[queue][conf]
    configs["mapred-site.xml"].configs["mapred.queue.names"] = queue_list
    return configs
    
if __name__ == "__main__":
    import sys

    if (len(sys.argv) != 2):
        print("Usage: " + __file__ + " <directory>")
        exit(1)

    fname = sys.argv[1]
    tmp = create_queues(fname)
    for queue in tmp:
        for opt in tmp[queue]:
            print(queue + " " + opt + ": " + tmp[queue][opt])
    tmp = create_configs(fname)
    for conf in tmp:
        print(tmp[conf].to_xml())
