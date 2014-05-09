import re
import os
from xml.etree import ElementTree as ET
from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader


class Config:
    """ Holds a Hadoop configuration.

    Config holds a Hadoop configuration as a two-dimensional dict. It
    is organized with the first dimension as the configuration key. The
    second dimension contains two elements: value and final.

    Config allows for returning either an XML or YAML representation.

    """

    def __init__(self):
        self.conf = dict()

    @classmethod
    def from_xml(cls, filename):
        """ Parses Hadoop XML and fill a two-dimensonal map. """

        conf = cls()
        key = ''
        val = ''
        fnl = False

        for event, elem in ET.iterparse(filename):
            if elem.tag == 'name':
                if len(key) > 0:
                    conf[key, Config.val_tag] = val
                    conf[key, Config.fnl_tag] = fnl
                key = elem.text
                val = ''
                fnl = False
            elif elem.tag == 'value':
                val = elem.text
            elif elem.tag == 'final':
                fnl = elem.text
        conf[key, Config.val_tag] = val
        conf[key, Config.fnl_tag] = fnl
        return conf

    @classmethod
    def from_yaml(cls, filename):
        conf = cls()
        conf.add_yaml_file(filename)
        return conf

    def add_yaml_file(self, filename):
        """Parse YAML and fill two-dimensional map."""

        key = ''
        val = ''
        fnl = ''

        f = open(filename, "r")
        data = load(f, Loader=Loader)

        for elem in data:
            key = elem
            try:
                val = data[elem][Config.val_tag]
                fnl = data[elem][Config.fnl_tag]
            except TypeError:
                val = data[elem]
                fnl = False
            except KeyError:
                print("The only acceptable subkeys are {" + ','.join(Config.subtags) + "}")
                print("Please fix " + elem + " in " + filename)
                exit(1)

            self[key, Config.val_tag] = val
            self[key, Config.fnl_tag] = fnl

    def to_xml(self):
        """ Return an XML representation of this Config """
        out = ["<configuration>"]
        for key in sorted(self.conf.keys()):
            out.append("\t<property>")
            out.append("\t\t<name>" + key + "</name>")
            out.append("\t\t<value>" + self[key, Config.val_tag] + "</value>")
            fnl = str(self[key, Config.fnl_tag]).lower()
            out.append("\t\t<final>" + fnl + "</final>")
            out.append("\t</property>")
        out.append("</configuration>")
        return '\n'.join(out)

    def to_yaml(self):
        return dump(self.conf, Dumper=Dumper, default_flow_style=False)

    def __str__(self):
        """ Returns a string representation. Debugging only """

        out = []
        for key in sorted(self.conf.keys()):
            tmp = []
            if self[key, Config.fnl_tag]:
                tmp.append('final')
            tmp.append('\t')
            tmp.append(key)
            tmp.append(':\t')
            tmp.append(self[key, Config.val_tag])
            out.append(''.join(tmp))
        return '\n'.join(out)

    def __getitem__(self, key):
        """ This documentation should be better """

        sub_key = Config.val_tag

        if type(key) is tuple:
            sub_key = key[1]
            key = key[0]
        key = str(key)
        sub_key = str(sub_key)

        if key not in self.conf.keys():
            raise KeyError("Key " + key + " doesn't exist in the config")

        return self.conf[key][sub_key]

    def __setitem__(self, key, value):
        """ Sets values """

        sub_key = Config.val_tag

        if type(key) is tuple:
            sub_key = key[1]
            key = key[0]
        key = str(key)
        sub_key = str(sub_key)

        if sub_key not in Config.subtags:
            raise KeyError("You cannot edit the " + sub_key + " attribute.")

        if key not in self.conf.keys():
            self.conf[key] = dict()
            self.conf[key][Config.val_tag] = ""
            self.conf[key][Config.fnl_tag] = False

        if sub_key == Config.val_tag:
            self.conf[key][sub_key] = str(value)
        elif sub_key == Config.fnl_tag:
            self.conf[key][sub_key] = bool(value)

    val_tag = 'value'
    fnl_tag = 'final'
    subtags = (val_tag, fnl_tag)

class Mapper:

    def __init__(self, rep, field_sep, mapping=dict()):
        self.rep = rep
        self.field_sep = field_sep
        self.mapping = dict()
        for key in mapping:
            self.mapping[key] = hadmin.mapping.fwd[key]

    def __setitem__(self, key, value):
        self.mapping[key] = value
        self.mapping[value] = key

    def __getitem__(self, key):
        ret = None
        if type(key) is tuple:
            match = re.sub(self.rep, key[1], self.mapping[key[0]])
            if not match:
                raise KeyError(expr + ' is improperly mapped')
            ret = str(match)
        else:
            key_fixed = self.find_bare_key(key)
            if key_fixed == key:
                ret = self.mapping[key]
            else:
                ret = (key.split(self.field_sep)[-2], self.mapping[key_fixed])
        return ret

    def find_bare_key(self, key):
        if key in self.mapping.keys():
            return key
        key_split = key.split(self.field_sep)
        if len(key_split) > 2:
            key_split[-2] = self.rep
        key_fixed = self.field_sep.join(key_split)
        return key_fixed

class QueueACLConfig(Config):
    """ Holds a CapacityScheduler queue config, specifically the one in
    mapred-queue-acls.xml.

    The class generates the verbose XML configuration from a lighter
    YAML format used by Hadmin. """

    def __init__(self):
        self.conf = dict()
        self.queue_list = list()

    @classmethod
    def from_yaml(cls, fname):
        conf = cls()

        f = open(fname, "r")
        data = load(f, Loader=Loader)

        for queue in data:
            for key in ("users", "admins"):
                conf[conf.get_key(key, queue)] = data[queue][key]
            conf.queue_list.append(queue)
        return conf

    def to_yaml(self, hadmin_dict):
        for key in self.conf:
            tmp = key.split('.')
            hadmin_dict[tmp[-2]][QueueACLConfig.rev_key_map[tmp[-1]]] = self[key]

    def get_key(self, key, queue):
        match = re.sub(QueueACLConfig.key_rep, queue,
                       QueueACLConfig.key_map[key])
        if not match:
            raise KeyError(key + " is improperly mapped")
        return str(match)

    key_rep = "____"
    key_map = dict()
    key_map["users"] = "mapred.queue." + key_rep + ".acl-submit-job"
    key_map["admins"] = "mapred.queue." + key_rep + ".acl-administer-jobs"

    rev_key_map = dict()
    rev_key_map['acl-submit-job'] = 'users'
    rev_key_map['acl-administer-jobs'] = 'admins'


class CapacitySchedulerConfig(Config):
    """ Holds a CapacityScheduler configuration, specifically the config
    in capacity-scheduler.xml.

    This class generates the config partly from Hadmin YAML and partly
    from Hadoop YAML. """

    key_rep = "____"
    cap_pre = "mapred.capacity-scheduler.queue." + key_rep + "."
    key_map = dict()
    key_map["capacity"] = cap_pre + "capacity"
    key_map["max-cap"] = cap_pre + "maximum-capacity"
    key_map["max-init-tpu"] = cap_pre + "maximum-initialized-active-tasks-per-user"
    rev_key_map = dict()
    rev_key_map['capacity'] = 'capacity'
    rev_key_map['maximum-capacity'] = 'max-cap'
    rev_key_map['maximum-initialized-active-tasks-per-user'] = 'max-init-tpu'

    def __init__(self):
        self.conf = dict()
        self.queue_list = list()

    def get_key(self, key, queue):
        match = re.sub(CapacitySchedulerConfig.key_rep, queue,
                       CapacitySchedulerConfig.key_map[key])
        if not match:
            raise KeyError(key + " is improperly mapped")
        return str(match)

    @classmethod
    def from_yaml(cls, hadmin_file, capacity_file):
        conf = cls()
        f = open(hadmin_file, "r")
        data = load(f, Loader=Loader)

        for queue in data:
            for key in CapacitySchedulerConfig.key_map:
                conf.get_key(key, queue)
                conf[match] = data[queue][key]
            conf.queue_list.append(queue)

        conf.add_yaml_file(capacity_file)
        return conf

    def to_yaml(self, hadmin_dict):
        passed_out = list()
        if len(self.queue_list) == 0:
            self.gen_queue_list()
        for queue in self.queue_list:
            for key in CapacitySchedulerConfig.key_map:
                match = self.get_key(key, queue)
                passed_out.append(match)

        out = dict()
        for key in self.conf:
            if key in passed_out:
                tmp = key.split('.')
                hadmin_dict[tmp[-2]][CapacitySchedulerConfig.rev_key_map[tmp[-1]]] = self[key]
            else:
                out[key] = self[key]

        return dump(out, Dumper=Dumper, default_flow_style=False)

    def gen_queue_list(self):
        for key in self.conf:
            tmp = key.split('.')
            if tmp[-1] == 'capacity':
                self.queue_list.append(tmp[-2])

class ConfigManager(dict):
    """ Initializes and manages all the config files. """

    @classmethod
    def from_xml(cls, directory):
        mgr = cls()
        mgr['capacity-scheduler'] = CapacitySchedulerConfig.from_xml(directory + '/capacity-scheduler.xml')
        mgr['core-site'] = Config.from_xml(directory + "/core-site.xml")
        mgr['hadoop-policy'] = Config.from_xml(directory + "/hadoop-policy.xml")
        mgr['hdfs-site'] = Config.from_xml(directory + "/hdfs-site.xml")
        mgr['mapred-site'] = Config.from_xml(directory + "/mapred-site.xml")
        mgr['mapred-queue-acls'] = QueueACLConfig.from_xml(directory + "/mapred-queue-acls.xml")
        return mgr

    @classmethod
    def from_yaml(cls, directory):
        """ Creates all the config files. Adds queue and user info to
        capacity-scheduler.xml, mapred-queue-acls.xml, and mapred-site.xml. """

        mgr = mgr()
        mgr['capacity-scheduler'] = CapacitySchedulerConfig.from_yaml(directory + "/hadmin-queues.yaml", directory + "/capacity-scheduler.yaml")
        mgr['core-site'] = Config.from_yaml(directory + "/core-site.yaml")
        mgr['hadoop-policy'] = Config.from_yaml(directory + "/hadoop-policy.yaml")
        mgr['hdfs-site'] = Config.from_yaml(directory + "/hdfs-site.yaml")
        mgr['mapred-site'] = Config.from_yaml(directory + "/mapred-site.yaml")
        mgr['mapred-queue-amgr'] = QueueACLConfig.from_yaml(directory + "/hadmin-queues.yaml")

        mgr['mapred-site']['mapred.queue.names'] = ','.join(mgr['mapred-queue-amgr.xml'].queue_list)
        return mgr

    def generate(self, directory):
        """ Generates all the XML Hadoop config """

        try:
            os.listdir(directory)
        except FileNotFoundError:
            os.mkdir(directory)

        for filename in self:
            f = open(directory + '/' + filename + '.xml', 'w')
            f.write(self[filename].to_xml())
            f.close()

    def save(self, directory):
        """ Save as HAdmin YAML """
        hadmin_file = dict()
        self['capacity-scheduler'].gen_queue_list()
        for queue in self['capacity-scheduler'].queue_list:
            hadmin_file[queue] = dict()

        for filename in self:
            write = True
            out = ''
            if filename == 'capacity-scheduler':
                out = self[filename].to_yaml(hadmin_file)
            elif filename == 'mapred-queue-acls':
                self[filename].to_yaml(hadmin_file)
                write = False
            else:
                out = self[filename].to_yaml()

            if write:
                with open(directory + '/' + filename + '.yaml', 'w') as f:
                    f.write(out)

        with open(directory + '/hadmin-queues.yaml', 'w') as f:
            f.write(dump(hadmin_file, default_flow_style=False, Dumper=Dumper))

class HadminManager:
    """ Manages modifying users and queues easy programatically
    
    Note: Keeps the lists of users and admins sorted for better vcs
    control

    """

    def __init__(self, directory):
        self.filename = directory + '/hadmin-queues.yaml'
        with open(self.filename, 'r') as f:
            self.conf = load(f, Loader=Loader)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        with open(self.filename, 'w') as f:
            f.write(dump(self.conf, Dumper=Dumper, default_flow_style=False))

    def check_queue(self, queue):
        """ Checks that a queue exists """
        if queue not in self.conf.keys():
            raise KeyError('Queue ' + queue + ' does not exist')

    def add_user_or_admin(self, ident, user, queue):
        """ Adds a user/admin to a queue """
        if ident not in ('users', 'admins'):
            raise ValueError('ident ' + ident + ' incorrect')
        self.check_queue(queue)
        arr = self.conf[queue][ident].split(',')
        if user in arr:
            raise KeyError(ident[0:-1].capitalize() + ' ' + user + ' is already in queue ' + queue)

        arr.append(user)
        self.conf[queue][ident] = ','.join(sorted(arr))

    def add_user(self, user, queue):
        self.add_user_or_admin('users', user, queue)

    def add_admin(self, admin, queue):
        self.add_user_or_admin('admins', admin, queue)

    def del_user_or_admin(self, ident, user, queue):
        """ Deletes a user from a queue """
        if ident not in ('users', 'admins'):
            raise ValueError('ident ' + ident + ' incorrect')
        self.check_queue(queue)
        try:
            arr = self.conf[queue][ident].split(',')
            if len(arr) < 2:
                raise AttributeError('Cannot delete the last ' + ident[0:-1] + ' of a queue')
            del(arr[arr.index(user)])
            self.conf[queue][ident] = ','.join(sorted(arr))
        except ValueError:
            raise ValueError(ident[0:-1].capitalize() + ' ' + user + ' is not in queue ' + queue)

    def del_user(self, user, queue):
        self.del_user_or_admin('users', user, queue)

    def del_admin(self, user, queue):
        self.del_user_or_admin('admins', user, queue)

    def add_queue(self, queue, user):
        if queue in self.conf.keys():
            raise KeyError('Queue ' + queue + ' already exists')

        self.conf[queue] = dict()
        self.conf[queue]['users'] = user
        self.conf[queue]['admins'] = user
        self.conf[queue]['capacity'] = 0
        self.conf[queue]['max-cap'] = 0
        self.conf[queue]['max-init-tpu'] = 0

    def del_queue(self, queue):
        if queue not in self.conf.keys():
            raise KeyError('Queue ' + queue + ' does not exist')

        del(self.conf[queue])

    def set_queue_cap(self, queue, cap):
        self.check_queue(queue)
        tmp = int(cap)
        self.conf[queue]['capacity'] = tmp

    def set_queue_max_cap(self, queue, max_cap):
        self.check_queue(queue)
        tmp = int(max_cap)
        self.conf[queue]['max-cap'] = tmp

    def set_queue_max_init_tpu(self, queue, max_init_tpu):
        self.check_queue(queue)
        tmp = int(max_init_tpu)
        self.conf[queue]['max-init-tpu'] = tmp


# Execute a small demo if run as a script
if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: " + __file__ + " <queue_conf> <cap_conf>")
        exit(1)

    conf = ConfigManager(sys.argv[1])
    for f in conf:
        print(f)
        print(conf[f])
        print()
