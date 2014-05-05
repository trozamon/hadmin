import re
from xml.etree import ElementTree as ET
from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader


class Config:
    """ Holds a Hadoop configuration.

    Config holds a Hadoop configuration as a two-dimensional map. It
    is organized with the first dimension as the configuration key. The
    second dimension contains two elements: value and final.

    Config allows for returning either an XML or YAML representation.
    Future work will be to include type-checking and other potential
    validation based on a rules file, although this will be in a different
    class, probably ConfigValidator or something.

    """

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
                    conf[key][Config.val_tag] = val
                    conf[key][Config.fnl_tag] = fnl
                key = elem.text
                val = ''
                fnl = False
            elif elem.tag == 'value':
                val = elem.text
            elif elem.tag == 'final':
                fnl = elem.text
        conf.validate()
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

    def add_key(self, key):
        """ Adds a new config key if it doesn't already exist. This
        is mostly to be used internally """

        if key not in self.conf.keys():
            self.conf[key] = dict()
            self.conf[key][Config.val_tag] = ""
            self.conf[key][Config.fnl_tag] = False

    def __init__(self):
        """ Does what little initialization there is. """

        self.conf = dict()

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

        if key not in self.conf.keys():
            raise KeyError("Key " + key + " doesn't exist in the config")

        return self.conf[key][sub_key]

    def __setitem__(self, key, value):
        """ Sets values """

        sub_key = Config.val_tag

        if type(key) is tuple:
            sub_key = key[1]
            key = key[0]

        if sub_key not in Config.subtags:
            raise KeyError("You cannot edit the " + sub_key + " attribute.")

        if key not in self.conf.keys():
            self.add_key(key)

        if sub_key == Config.val_tag:
            self.conf[key][sub_key] = str(value)
        elif sub_key == Config.fnl_tag:
            self.conf[key][sub_key] = bool(value)

    val_tag = 'value'
    fnl_tag = 'final'
    subtags = (val_tag, fnl_tag)


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

    @classmethod
    def from_yaml(cls, hadmin_file, capacity_file):
        conf = cls()
        f = open(hadmin_file, "r")
        data = load(f, Loader=Loader)

        for queue in data:
            for key in ("capacity", "max-cap", "max-init-tpu"):
                match = re.sub(CapacitySchedulerConfig.key_rep, queue,
                               CapacitySchedulerConfig.key_map[key])
                if not match:
                    raise KeyError(key + " is improperly mapped")
                conf[match] = data[queue][key]

        conf.add_yaml_file(capacity_file)
        return conf


class ConfigManager(dict):
    """ Initializes and manages all the config files. """

    def __init__(self, directory):
        """ Creates all the config files. Adds queue and user info to
        capacity-scheduler.xml, mapred-queue-acls.xml, and mapred-site.xml. """

        self['capacity-scheduler.xml'] = CapacitySchedulerConfig.from_yaml(directory + "/hadmin-queues.yaml", directory + "/capacity-scheduler.yaml")
        self['core-site.xml'] = Config.from_yaml(directory + "/core-site.yaml")
        self['hadoop-policy.xml'] = Config.from_yaml(directory + "/hadoop-policy.yaml")
        self['hdfs-site.xml'] = Config.from_yaml(directory + "/hdfs-site.yaml")
        self['mapred-site.xml'] = Config.from_yaml(directory + "/mapred-site.yaml")
        self['mapred-queue-acls.xml'] = QueueACLConfig.from_yaml(directory + "/hadmin-queues.yaml")

        self['mapred-site.xml']['mapred.queue.names'] = ','.join(self['mapred-queue-acls.xml'].queue_list)

    def generate(self, directory):
        """ Generates all the XML Hadoop config """

        for filename in self:
            f = open(directory + '/' + filename, 'w')
            f.write(self[filename].to_xml())
            f.close()

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
