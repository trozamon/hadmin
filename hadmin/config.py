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
        self.add(data)

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
                raise KeyError("You must have a '" + Config.val_tag +
                               "' sub-key for " + key)

        for key in data:
            try:
                data[key][Config.fnl_tag]
            except KeyError:
                data[key][Config.fnl_tag] = False

    def keys(self):
        return self.conf.keys()

    def to_xml(self):
        """ Return an XML representation of this Config. """

        out = ["<configuration>"]
        for key in sorted(self.conf.keys()):
            out.append("\t<property>")
            out.append("\t\t<name>" + key + "</name>")
            out.append("\t\t<value>" + self[key, Config.val_tag] + "</value>")
            if self[key, Config.fnl_tag]:
                out.append("\t\t<final>true</final>")
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


class Manager(dict):
    """ Initializes and manages all the config files. """

    @classmethod
    def from_yaml(cls, directory, ver):
        """ Creates all the config files. Adds queue and user info to
        capacity-scheduler.xml, mapred-queue-acls.xml, and mapred-site.xml. """

        mgr = cls()

        with Internal.from_dir(directory) as thing:
            mgr['capacity-scheduler'] = thing.get_config('scheduler', ver)
            if ver == 1:
                mgr['mapred-queue-acls'] = thing.get_config('queues', ver)
                mgr['mapred-queue-acls']['mapred.queue.names'] = \
                        thing.queue_list_str()
            elif ver == 2:
                mgr['mapred-queues'] = thing.get_config('queues', ver)
        return mgr

    def generate(self, directory):
        """ Generates all the XML Hadoop config """

        try:
            os.listdir(directory)
        except FileNotFoundError:
            os.mkdir(directory)

        for filename in self:
            with open(directory + '/' + filename + '.xml', 'w') as f:
                f.write(self[filename].to_xml())


class Internal:
    """ Manages modifying users and queues easy programatically

    Note: Keeps the lists of users and admins sorted for better vcs
    control

    """

    _ownership = {
        1: {
            'scheduler': [
                ('scheduler', 'mapred.capacity-scheduler.default-init-accept-jobs-factor'),
                ('scheduler', 'mapred.capacity-scheduler.default-user-limit-factor'),
                ('scheduler', 'mapred.capacity-scheduler.default-supports-priority'),
                ('scheduler', 'mapred.capacity-scheduler.init-poll-interval'),
                ('scheduler', 'mapred.capacity-scheduler.init-worker-threads'),
                ('scheduler', 'maxjobs'),
                ('scheduler', 'maxtpq'),
                ('scheduler', 'maxtpu'),
                ('scheduler', 'ulim'),
                ('queues', 'mincap'),
                ('queues', 'maxcap'),
                ('queues', 'ulim'),
                ('queues', 'maxtpu')
                ],
            'queues': [
                ('queues', 'admins'),
                ('queues', 'users')
                ]
            },
        2: {
            'scheduler': [
                ('scheduler', 'maxjobs'),
                ('scheduler', 'yarn.scheduler.capacity.maximum-am-resource-percent'),
                ('scheduler', 'yarn.scheduler.capacity.node-locality-delay'),
                ('scheduler', 'yarn.scheduler.capacity.resource-calculator'),
                ('queues', 'mincap'),
                ('queues', 'maxcap'),
                ('queues', 'admins'),
                ('queues', 'users'),
                ('queues', 'state'),
                ('queues', 'ulim')
                ],
            'queues': []
            }
        }

    @classmethod
    def from_dir(cls, directory):
        filename = directory + '/hadmin.yaml'
        with open(filename, 'r') as f:
            tmp = cls(load(f, Loader=Loader))

        tmp.filename = filename
        return tmp

    def __init__(self, data):
        self.conf = copy.deepcopy(data)
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
        if queue not in self.conf['queues'].keys():
            raise KeyError('Queue ' + queue + ' does not exist')

    def add_user(self, user, queue):
        self.__add(('queues', 'users'), user, queue)

    def add_admin(self, admin, queue):
        self.__add(('queues', 'admins'), admin, queue)

    def del_user(self, user, queue):
        self.__remove(('queues', 'users'), user, queue)

    def del_admin(self, admin, queue):
        self.__remove(('queues', 'admins'), admin, queue)

    def add_queue(self, queue, user):
        if queue in self.conf['queues'].keys():
            raise KeyError('Queue ' + queue + ' already exists')

        self.conf['queues'][queue] = dict()
        self.__set(('queues', 'users'), user, queue)
        self.__set(('queues', 'admins'), user, queue)
        self.__set(('queues', 'mincap'), 0, queue)
        self.__set(('queues', 'maxcap'), 0, queue)
        self.__set(('queues', 'maxtpu'), 0, queue)

    def del_queue(self, queue):
        if queue not in self.conf['queues'].keys():
            raise KeyError('Queue ' + queue + ' does not exist')

        del(self.conf['queues'][queue])

    def set_queue_cap(self, queue, cap):
        self.check_queue(queue)
        tmp = int(cap)
        self.__set(('queues', 'mincap'), tmp, queue)

    def set_queue_max_cap(self, queue, max_cap):
        self.check_queue(queue)
        tmp = int(max_cap)
        self.__set(('queues', 'maxcap'), tmp, queue)

    def set_queue_max_init_tpu(self, queue, max_init_tpu):
        self.check_queue(queue)
        tmp = int(max_init_tpu)
        self.__set(('queues', 'maxtpu'), tmp, queue)

    def get_config(self, key, ver):
        out = Config()
        conf = self.get_data(key, ver)
        mapper = hadmin.mapping.HadoopMapper()
        for owner in conf:
            if owner == 'queues':
                for queue in conf[owner]:
                    for tmp in conf[owner][queue]:
                        final_key = re.sub(hadmin.mapping.HadoopMapper.rep,
                                           queue,
                                           mapper[tmp, ver, owner])
                        if not final_key:
                            raise KeyError(mapper[tmp, ver, owner] + ' is not ' +
                                    'meant for queues')
                        out[final_key] = conf[owner][queue][tmp]
            else:
                for tmp in conf[owner]:
                    final_key = mapper[tmp, ver, owner]
                    out[final_key] = conf[owner][tmp]

        return out

    def get_data(self, key, ver):
        out = dict()
        out['queues'] = dict()
        out['scheduler'] = dict()

        for queue in self.queue_list():
            out['queues'][queue] = dict()

        for key in Internal._ownership[ver][key]:
            if key[0] == 'queues':
                for queue in self.conf['queues']:
                    try:
                        out['queues'][queue][key[1]] = \
                                self.conf['queues'][queue][key[1]]
                    except KeyError:
                        raise KeyError('You need to define the ' + key[1] +
                                       ' in the queues section')
            else:
                try:
                    out[key[0]][key[1]] = self.conf[key[0]][key[1]]
                except KeyError:
                    raise KeyError('You need to define the ' + key[1] +
                                   ' key in the ' + key[0] + ' section')

        return out

    def queue_list(self):
        return sorted(self.conf['queues'].keys())

    def queue_list_str(self):
        return ','.join(self.queue_list())

    def __add(self, args, value, queue=None):
        """ Appends a value to something in the config """

        owner = args[0]
        key = args[1]

        if owner == 'queues' and queue is None:
            raise KeyError("You can't specify a queue operation and not tell \
                    me which queue")

        tmp = None
        if queue is not None:
            tmp = self.conf[owner][queue][key].split(',')
        else:
            tmp = self.conf[owner][key].split(',')

        if value in tmp:
            raise ValueError(value + ' is already in ' + key)
        tmp.append(value)
        tmp = ','.join(sorted(tmp))

        if queue is not None:
            self.conf[owner][queue][key] = tmp
        else:
            self.conf[owner][key] = tmp

    def __remove(self, args, value, queue=None):
        """ Removes a value from something in the config """

        owner = args[0]
        key = args[1]

        if owner == 'queues' and queue is None:
            raise KeyError("You can't specify a queue operation and not tell \
                    me which queue")

        tmp = None
        if queue is not None:
            tmp = self.conf[owner][queue][key].split(',')
        else:
            tmp = self.conf[owner][key].split(',')

        if len(tmp) == 1:
            raise ValueError('You cannot attempt to delete the last ' + key + \
                    ' in a list')

        for i in range(0, len(tmp)):
            if tmp[i] == value:
                del(tmp[i])

        tmp = ','.join(sorted(tmp))

        if queue is not None:
            self.conf[owner][queue][key] = tmp
        else:
            self.conf[owner][key] = tmp

    def __set(self, args, value, queue=None):
        """ Appends a value to something in the config """

        owner = args[0]
        key = args[1]

        if owner == 'queues' and queue is None:
            raise KeyError("You can't specify a queue operation and not tell \
                    me which queue")

        if queue is not None:
            self.conf[owner][queue][key] = value
        else:
            self.conf[owner][key] = value

class TypeChecker:

    def check(self, key, val):
        ret = False
        try:
            ret = TypeChecker._funcs[TypeChecker._types[key]](self, val)
        except KeyError:
            raise KeyError(key + ' does not have a defined type')
        return ret

    def _check_num(self, val):
        if type(val) in (int,float):
            return True
        return False

    def _check_str(self, val):
        if type(val) is str and ',' not in val:
            return True
        return False

    def _check_csv(self, val):
        if type(val) is str:
            return True
        return False

    _types = {
            'ulim': 'num',
            'maxjobs': 'num',
            'maxtpu': 'num',
            'maxtpq': 'num',
            'mincap': 'num',
            'maxcap': 'num',
            'admins': 'csv',
            'users': 'csv',
            'running': 'str'
            }

    _funcs = {
            'num': _check_num,
            'csv': _check_csv,
            'str': _check_str
            }
