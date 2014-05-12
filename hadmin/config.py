import re
import os
import copy
import hadmin.mapping
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

        f = open(filename, 'r')
        data = load(f, Loader=Loader)
        add(data)

    def add(self, data):
        """ Adds the contents of a two-dimensional dict to this Config. """

        self.fixup(data)

        for key in data:
            self[key, Config.val_tag] = data[key][Config.val_tag]
            self[key, Config.fnl_tag] = data[key][Config.fnl_tag]

    def fixup(self, data):
        """ Fixes up data that comes in that should be a two-dimensional
        dict """

        for key in data:
            try:
                data[key][Config.val_tag]
            except TypeError:
                tmp = data[key]
                data[key] = {Config.val_tag: tmp, Config.fnl_tag: False}
            except KeyError:
                raise KeyError("You must have a '" + Config.val_tag + \
                        "' sub-key for " + key)

        for key in data:
            try:
                data[key][Config.fnl_tag]
            except KeyError:
                data[key][Config.fnl_tag] = False

    def to_xml(self):
        """ Return an XML representation of this Config. """

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


class Manager(dict):
    """ Initializes and manages all the config files. """

    @classmethod
    def from_xml(cls, directory):

        mgr = cls()
        mgr['capacity-scheduler'] = \
                Config.from_xml(directory + '/capacity-scheduler.xml')
        mgr['core-site'] = Config.from_xml(directory + "/core-site.xml")
        mgr['hadoop-policy'] = \
                Config.from_xml(directory + "/hadoop-policy.xml")
        mgr['hdfs-site'] = Config.from_xml(directory + "/hdfs-site.xml")
        mgr['mapred-site'] = Config.from_xml(directory + "/mapred-site.xml")
        mgr['mapred-queue-acls'] = \
                Config.from_xml(directory + "/mapred-queue-acls.xml")
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

class Internal:
    """ Manages modifying users and queues easy programatically
    
    Note: Keeps the lists of users and admins sorted for better vcs
    control

    """

    @classmethod
    def from_dir(cls, directory):
        filename = directory + '/hadmin-queues.yaml'
        with open(filename, 'r') as f:
            tmp = cls(load(f, Loader=Loader))

        tmp.filename = filename
        return conf

    def __init__(self, data):
        self.conf = copy.deepcopy(data)
        self.queues = self.conf['queues']
        self.filename = ''

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if len(self.filename) > 0:
            with open(self.filename, 'w') as f:
                f.write(dump(self.conf, Dumper=Dumper,
                    default_flow_style=False))

    def check_queue(self, queue):
        """ Checks that a queue exists """
        if queue not in self.queues.keys():
            raise KeyError('Queue ' + queue + ' does not exist')

    def add_user_or_admin(self, ident, user, queue):
        """ Adds a user/admin to a queue """
        if ident not in ('users', 'admins'):
            raise ValueError('ident ' + ident + ' incorrect')
        self.check_queue(queue)
        arr = self.queues[queue][ident].split(',')
        if user in arr:
            raise KeyError(ident[0:-1].capitalize() + ' ' + user + ' is already in queue ' + queue)

        arr.append(user)
        self.queues[queue][ident] = ','.join(sorted(arr))

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
            arr = self.queues[queue][ident].split(',')
            if len(arr) < 2:
                raise AttributeError('Cannot delete the last ' + ident[0:-1] + ' of a queue')
            del(arr[arr.index(user)])
            self.queues[queue][ident] = ','.join(sorted(arr))
        except ValueError:
            raise ValueError(ident[0:-1].capitalize() + ' ' + user + ' is not in queue ' + queue)

    def del_user(self, user, queue):
        self.del_user_or_admin('users', user, queue)

    def del_admin(self, user, queue):
        self.del_user_or_admin('admins', user, queue)

    def add_queue(self, queue, user):
        if queue in self.queues.keys():
            raise KeyError('Queue ' + queue + ' already exists')

        self.queues[queue] = dict()
        self.queues[queue]['users'] = user
        self.queues[queue]['admins'] = user
        self.queues[queue]['capacity'] = 0
        self.queues[queue]['max-cap'] = 0
        self.queues[queue]['max-tpu'] = 0

    def del_queue(self, queue):
        if queue not in self.queues.keys():
            raise KeyError('Queue ' + queue + ' does not exist')

        del(self.queues[queue])

    def set_queue_cap(self, queue, cap):
        self.check_queue(queue)
        tmp = int(cap)
        self.queues[queue]['capacity'] = tmp

    def set_queue_max_cap(self, queue, max_cap):
        self.check_queue(queue)
        tmp = int(max_cap)
        self.queues[queue]['max-cap'] = tmp

    def set_queue_max_init_tpu(self, queue, max_init_tpu):
        self.check_queue(queue)
        tmp = int(max_init_tpu)
        self.queues[queue]['max-tpu'] = tmp

    def get_config(self, key):
        out = Config()
        mapper = Mapper(field_sep=hadmin.mapping.field_sep,
                rep=hadmin.mapping.rep, mapping=hadmin.mapping.fwd)
        for own in hadmin.mapping.ownership[key]:
            if type(own) is tuple:
                tmp = self.conf[own[0]]
                for queue in tmp:
                    key = re.sub(mapper.rep, queue, mapper[own[1]])
                    out[key] = tmp[queue][own[1]]
            elif own in self.conf.keys():
                out[mapper[own]] = self.conf[own]
        return out

    def queue_list(self):
        return ','.join(sorted(self.conf['queues'].keys()))
