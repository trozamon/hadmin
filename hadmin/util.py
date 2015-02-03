"""
HAdmin is an API and command line tool to configure the queues of Hadoop's
CapacityScheduler.
"""

from argparse import ArgumentParser
import xml.etree.ElementTree as ET
import os
import subprocess
import sys


scheduler_fname = 'capacity-scheduler.xml'
if scheduler_fname not in os.listdir('.'):
    scheduler_fname = '/'.join([
        '/etc/hadoop/conf',
        scheduler_fname
        ])

pre_scheduler = 'yarn.scheduler.capacity'
post_users = 'acl_submit_applications'
post_admins = 'acl_administer_queue'
post_cap = 'capacity'
post_maxcap = 'maximum-capacity'
post_state = 'state'
post_ulim = 'user-limit-factor'
post_subs = 'queues'


def queue_fqn(queue='root'):
    """ Returns the fully qualified name of a queue. """
    if ',' in queue:
        raise KeyError('Queue names may not contain commas')

    if (queue[0:4] == 'root'):
        return queue
    return ''.join(['root.', queue])


def queue_parent(queue='root'):
    """ Returns the parent of a queue. """
    return '.'.join(queue_fqn(queue).split('.')[0:-1])


def queue_cap_fqn(queue='root'):
    """ Returns the config key specifying the capacity of a queue. """
    return '.'.join([pre_scheduler, queue_fqn(queue), post_cap])


def queue_maxcap_fqn(queue='root'):
    """ Returns the config key specifying the max capacity of a queue. """
    return '.'.join([pre_scheduler, queue_fqn(queue), post_maxcap])


def queue_users_fqn(queue='root'):
    """ Returns the config key specifying the users of a queue. """
    return '.'.join([pre_scheduler, queue_fqn(queue), post_users])


def queue_admins_fqn(queue='root'):
    """ Returns the config key specifying the admins of a queue. """
    return '.'.join([pre_scheduler, queue_fqn(queue), post_admins])


def queue_state_fqn(queue='root'):
    """ Returns the config key specifying the state of a queue. """
    return '.'.join([pre_scheduler, queue_fqn(queue), post_state])


def queue_ulim_fqn(queue='root'):
    """ Returns the config key specifying the ulim of a queue. """
    return '.'.join([pre_scheduler, queue_fqn(queue), post_ulim])


def queue_subs_fqn(queue='root'):
    """ Returns the config key specifying the subqueues of a queue. """
    return '.'.join([pre_scheduler, queue_fqn(queue), post_subs])


class HXML:
    """
    A wrapper around etree specific to Hadoop's XML format.

    Provides an easy way to deal with Hadoop's XML so that calling classes
    only worry about Hadoop config keys and values, and not dealing with all
    the cruft. Unless you're crazy, avoid using this class directly and instead
    use a class such as QueueManager to fulfill your needs.
    """

    def __init__(self, etree):
        self.tree = etree

    @classmethod
    def from_etree(cls, etree):
        """ Not sure why this is here since __init__() takes etrees. """
        ret = cls(etree)
        return ret

    @classmethod
    def from_str(cls, string):
        """ Construct from a raw XML string. """
        ret = cls(ET.fromstring(string))
        return ret

    @classmethod
    def from_file(cls, fname):
        """ Construct from an XML file. """
        ret = cls(ET.parse(fname).getroot())
        return ret

    def __getitem__(self, prop):
        """ Retrieves a config value. """
        for node in self.tree.findall('property'):
            if node.find('name').text == prop:
                return node.find('value').text

        raise KeyError('Key ' + prop + ' not found')

    def __setitem__(self, prop, val):
        """
        Sets a config value. Please note that if the specified property
        prop does not exist, it will be created.
        """

        success = False

        if type(val) in (int, float):
            val = str(val)
        elif type(val) is bool:
            val = str(val).lower()

        for node in self.tree.findall('property'):
            if node.find('name').text == prop:
                node.find('value').text = val
                success = True

        if success is not True:
            el = ET.Element('property')
            el.text = "\n    "
            el.tail = "\n"
            name_el = ET.Element('name')
            name_el.tail = "\n    "
            name_el.text = prop
            el.append(name_el)
            val_el = ET.Element('value')
            val_el.tail = "\n  "
            val_el.text = val
            el.append(val_el)
            self.tree.append(el)

    def remove(self, prop):
        """ Deletes a property from the etree. """
        for node in self.tree.findall('property'):
            if node.find('name').text == prop:
                self.tree.remove(node)

    def keys(self):
        """ Returns a list of keys. """
        ret = list()
        for node in self.tree.findall('property'):
            ret.append(node.find('name').text)
        return sorted(ret)

    def save(self, fname):
        """ Saves to a file. """
        ET.ElementTree(self.tree).write(fname)


class QueueManager:
    """
    A class to specifically manage the queue's in Hadoop's CapacityScheduler.

    Contains a reasonable amount of functionality, and hopefully most of it
    is pretty common-sensical.
    """

    def __init__(self, hxml):
        self.hxml = hxml

    def __insert(self, prop, value):
        """ Inserts a value into a CSV string, checking for duplicates. """
        csv = [value]
        try:
            csv = self.hxml[prop].split(',')
            if value not in csv:
                csv.append(value)
        except KeyError:
            pass

        self.hxml[prop] = ','.join(sorted(csv))

    def __unsert(self, prop, value):
        """
        Unserts a value from a CSV string. I named this before realizing
        that 'remove' is actually the correct word. Good thing English is my
        only language.
        """

        csv = self.hxml[prop].split(',')

        if len(csv) == 1:
            raise ValueError("Can't delete last element in " + prop)

        for i in range(len(csv)):
            if csv[i] == value:
                del(csv[i])
                break

        self.hxml[prop] = ','.join(sorted(csv))

    def add_user(self, user, queue):
        """ Adds a user to a queue. """
        for user in user.split(','):
            self.__insert(queue_users_fqn(queue), user)

    def add_admin(self, user, queue):
        """ Adds an admin to a queue. """
        for user in user.split(','):
            self.__insert(queue_admins_fqn(queue), user)

    def set_cap(self, queue, cap):
        """ Sets a queue's capacity. """
        try:
            cap = int(cap)
        except ValueError:
            tmp = cap
            cap = int(queue)
            queue = tmp

        if cap > 100 or cap < 0:
            raise ValueError('Queue capacity must be between 0 and 100')

        self.hxml[queue_cap_fqn(queue)] = cap

    def set_maxcap(self, queue, maxcap):
        """ Set's a queue's maximum capacity. """
        try:
            maxcap = int(maxcap)
        except ValueError:
            tmp = maxcap
            maxcap = int(queue)
            queue = tmp

        if maxcap > 100 or maxcap < 0:
            raise ValueError(
                'Queue maximum capacity must be between 0 and 100'
                )

        self.hxml[queue_maxcap_fqn(queue)] = maxcap

    def set_ulim(self, queue, ulim):
        """ Set's a queue's user limit factor. """
        ulim = float(ulim)

        if ulim < 0.0:
            raise ValueError('ulim must be a positive float')

        self.hxml[queue_ulim_fqn(queue)] = ulim

    def set_state(self, queue, state):
        """ Set's a queue's state, which is either 'running' or 'stopped'. """
        self.hxml[queue_state_fqn(queue)] = state

    def off(self, queue):
        """ Convenience call to set a queue's state to 'stopped'. """
        self.set_state(queue, 'stopped')

    def on(self, queue):
        """ Convenience call to set a queue's state to 'running'. """
        self.set_state(queue, 'running')

    def add(self, queue, user):
        """ Adds a queue with begining user/admin combo. """
        if queue_fqn(queue) in self.queue_list():
            raise KeyError('Queue ' + queue + ' already exists')

        if len(user.split(',')) != 1:
            raise ValueError('Queues may be created with one admin only')

        parent = queue_parent(queue)
        self.__insert(queue_subs_fqn(parent), queue.split('.')[-1])

        self.add_user(user, queue)
        self.add_admin(user, queue)
        self.set_cap(queue, '1')
        self.set_maxcap(queue, '100')
        self.set_ulim(queue, '1.0')
        self.set_state(queue, 'running')

    def delete(self, queue):
        """ Deletes a queue like a boss. """
        parent = queue_parent(queue)
        self.__unsert(queue_subs_fqn(parent), queue)
        self.hxml.remove(queue_users_fqn(queue))
        self.hxml.remove(queue_admins_fqn(queue))
        self.hxml.remove(queue_cap_fqn(queue))
        self.hxml.remove(queue_maxcap_fqn(queue))
        self.hxml.remove(queue_ulim_fqn(queue))
        self.hxml.remove(queue_state_fqn(queue))

    def del_user(self, user, queue):
        """ Removes a user from a queue. """
        self.__unsert(queue_users_fqn(queue), user)

    def del_admin(self, admin, queue):
        """ Removes an admin from a queue. """
        self.__unsert(queue_admins_fqn(queue), admin)

    def save(self, fname):
        """ Saves the config. """
        self.hxml.save(fname)

    def queue_list(self, queue='root'):
        """ Generates and returns a list of queues. """
        ret = list()

        tmp = self.hxml.keys()
        tmp_cap = queue_cap_fqn(queue)
        tmp_subs = queue_subs_fqn(queue)
        if tmp_cap not in tmp and tmp_subs not in tmp:
            raise KeyError(queue + ' is not a valid queue')

        ret.append(queue_fqn(queue))

        try:
            for sub in self.hxml[queue_subs_fqn(queue)].split(','):
                ret = ret + self.queue_list('.'.join([queue, sub]))
        except KeyError:
            pass

        return sorted(ret)

    def user_list(self, queue='root'):
        """ Returns a list of users of a queue and subqueues. """
        ret = list()
        for sub in self.queue_list(queue):
            try:
                for user in self.hxml[queue_users_fqn(sub)].split(','):
                    if user not in ret:
                        ret.append(user)
            except KeyError:
                pass

        return sorted(ret)

    def admin_list(self, queue='root'):
        """ Returns a list of admins of a queue and subqueues. """
        ret = list()
        for sub in self.queue_list(queue):
            try:
                for user in self.hxml[queue_admins_fqn(sub)].split(','):
                    if user not in ret:
                        ret.append(user)
            except KeyError:
                pass

        return sorted(ret)

    def sc_caps(self):
        """
        Sanity check for the queue capacities. Checks that all queues on the
        same level have capacities that add to 100.
        """

        queues = self.queue_list()
        caps = dict()

        try:
            if self.hxml[queue_cap_fqn('root')] != '100':
                raise ValueError('Capacity of root is not 100')
        except KeyError:
            raise ValueError('Capacity of root is not 100')

        if self.hxml[queue_maxcap_fqn('root')] != '100':
            raise ValueError('Maximum capacity of root is not 100')

        for i in range(len(queues)):
            if queues[i] == 'root':
                del(queues[i])
                break

        for queue in queues:
            caps[queue_parent(queue)] = 0

        for queue in queues:
            caps[queue_parent(queue)] = caps[queue_parent(queue)] + \
                int(self.hxml[queue_cap_fqn(queue)])

        ret = list()
        for queue in caps:
            if caps[queue] != 100:
                ret.append(queue)

        return sorted(ret)

    def sc_maxcaps(self):
        """
        Sanity check for the queue maximum capacities. Ensures that each
        queue's maximum capacity is greater than or equal to its capacity.
        """

        queues = self.queue_list()
        ret = list()

        for queue in queues:
            cap = int(self.hxml[queue_cap_fqn(queue)])
            max_cap = int(self.hxml[queue_maxcap_fqn(queue)])
            if cap > max_cap:
                ret.append(queue)

        return sorted(ret)

    def sc_users(self, passwd):
        """
        Sanity check for the users. Ensures that each user in a queue actually
        exists on the machine this is being run on, since that's where the
        CapacityScheduler is running.
        """

        ret = list()
        real_users = users_from_passwd(passwd)
        for user in self.user_list():
            if user not in real_users:
                ret.append(user)

        return sorted(ret)

    def sc_admins(self, passwd):
        """
        Sanity check for the admins. Ensures that each admin in a queue
        actually exists on the machine this is being run on, since that's where
        the CapacityScheduler is running.
        """

        ret = list()
        real_admins = users_from_passwd(passwd)
        for admin in self.admin_list():
            if admin not in real_admins:
                ret.append(admin)

        return sorted(ret)


def users_from_passwd(raw):
    """ Extracts a list of users from a passwd type file. """
    users = list()
    for line in raw.split('\n'):
        tmp = line.split(':')[0]
        if len(tmp) > 0:
            users.append(tmp)

    return sorted(users)


def reload_queues():
    """ Reloads queues if relevant sanity checks are passed. This calls other
    binaries using subprocess. """

    mgr = QueueManager(HXML.from_file(scheduler_fname))

    if len(mgr.sc_caps()) > 0 or len(mgr.sc_maxcaps()) > 0:
        print('Sanity checks of either the queue capacities or maximum ' +
              'capacities failed. Please run "hadmin sc" to determine ' +
              'what is causing this failure. Afterwards, you will have to ' +
              'manually tell yarn to reload the queues with "yarn rmadmin ' +
              '-refreshQueues')
        return 1

    ret = subprocess.call('which yarn', shell=True)
    if ret != 0:
        print('You do not have the yarn binary on your system')
        print("Please manually run 'yarn rmadmin -refreshQueues'")
        return ret

    ret = subprocess.call('yarn rmadmin -refreshQueues', shell=True)
    if ret != 0:
        print("Refreshing queues failed, please manually run 'yarn rmadmin" +
              "-refreshQueues'")
        return ret

    return 0


def add_user_hdfs(user):
    """ Adds user directories. This calls other binaries using subprocess. """
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


def useradd(args):
    """
    Takes in the args that come after 'useradd' on the command line and also
    and array of config files so it can make changes.

    """

    parser = ArgumentParser(prog='useradd',
                            description='HAdmin useradd utility')
    parser.add_argument('user')
    parser.add_argument('queue')
    parser.add_argument('--admin', dest='is_admin', action='store_const',
                        const=True, default=False,
                        help='Add an administrator')
    args = parser.parse_args(args)
    mgr = QueueManager(HXML.from_file(scheduler_fname))
    if args.is_admin:
        mgr.add_admin(args.user, args.queue)
        print("Added admin " + args.user + " to queue " + args.queue)
    else:
        mgr.add_user(args.user, args.queue)
        print("Added user " + args.user + " to queue " + args.queue)

    mgr.save(scheduler_fname)

    for u in args.user.split(','):
        add_user_hdfs(u)

    ret = reload_queues()

    return ret


def userdel(args):
    """
    Takes in a user and removes him from a queue.

    """

    parser = ArgumentParser(prog='userdel',
                            description='HAdmin userdel utility')
    parser.add_argument('user')
    parser.add_argument('queue')
    parser.add_argument('--admin', dest='is_admin', action='store_const',
                        const=True, default=False,
                        help='Delete an administrator')
    args = parser.parse_args(args)
    mgr = QueueManager(HXML.from_file(scheduler_fname))
    if args.is_admin:
        mgr.del_admin(args.user, args.queue)
        print("Removed admin " + args.user + " from queue " +
              args.queue)
    else:
        mgr.del_user(args.user, args.queue)
        print("Removed user " + args.user + " from queue " +
              args.queue)

    mgr.save(scheduler_fname)

    return reload_queues()


def queueadd(args):
    """ Adds a queue. """
    parser = ArgumentParser(prog='queueadd',
                            description='HAdmin queueadd utility')
    parser.add_argument('queue')
    parser.add_argument('user')
    args = parser.parse_args(args)

    mgr = QueueManager(HXML.from_file(scheduler_fname))

    confirm = raw_input(' '.join(['Are you sure you want to add queue',
                    args.queue, 'with initial user/admin', args.user + '?',
                    '[y/N] ']))

    if confirm != 'y' and confirm != 'Y':
        return 0

    mgr.add(args.queue, args.user)
    print('Added queue ' + args.queue +
          ' with initial user/admin ' + args.user)

    mgr.save(scheduler_fname)

    add_user_hdfs(args.user)

    return reload_queues()


def queuedel(args):
    """ Turns a queue off or deletes it based on args. """
    parser = ArgumentParser(prog='queueadd',
                            description='HAdmin queueadd utility')
    parser.add_argument('queue')
    parser.add_argument('--force', dest='force', action='store_const',
                        const=True, default=False,
                        help='Force the queue deletion')
    args = parser.parse_args(args)
    if args.force:
        mgr = QueueManager(HXML.from_file(scheduler_fname))
        mgr.delete(args.queue)
        mgr.save(scheduler_fname)
        print('Removed queue ' + args.queue)
        return reload_queues()

    return queueoff([args.queue])


def queueon(args):
    """ Turns a queue on. """
    parser = ArgumentParser(prog='queueon',
                            description='HAdmin queueon utility')
    parser.add_argument('queue')
    args = parser.parse_args(args)
    mgr = QueueManager(HXML.from_file(scheduler_fname))
    mgr.on(args.queue)
    mgr.save(scheduler_fname)
    print('Turned queue ' + args.queue + ' on')

    return reload_queues()


def queueoff(args):
    """ Turns a queue off. """
    parser = ArgumentParser(prog='queueoff',
                            description='HAdmin queueoff utility')
    parser.add_argument('queue')
    args = parser.parse_args(args)
    mgr = QueueManager(HXML.from_file(scheduler_fname))
    mgr.off(args.queue)
    mgr.save(scheduler_fname)
    print('Turned queue ' + args.queue + ' off')

    return reload_queues()


def queuecap(args):
    """ Sets a queue's capacity or maximum capacity. """
    parser = ArgumentParser(prog='queuecap',
                            description='HAdmin queuecap utility')
    parser.add_argument('queue')
    parser.add_argument('capacity')
    parser.add_argument('--max', dest='maxcap', action='store_const',
                        const=True, default=False,
                        help='Set maximum capacity')
    args = parser.parse_args(args)
    mgr = QueueManager(HXML.from_file(scheduler_fname))
    if args.maxcap:
        mgr.set_maxcap(args.queue, args.capacity)
    else:
        mgr.set_cap(args.queue, args.capacity)
    mgr.save(scheduler_fname)

    out = 'Set '
    if args.maxcap:
        out = out + 'maximum '
    out = out + 'capacity of queue ' + args.queue + ' to ' + args.capacity
    print(out)

    return reload_queues()


def queueulim(args):
    """ Sets a queue's ulim. """
    parser = ArgumentParser(prog='queueulim',
                            description='HAdmin queueulim utility')
    parser.add_argument('queue')
    parser.add_argument('ulim')
    args = parser.parse_args(args)
    mgr = QueueManager(HXML.from_file(scheduler_fname))
    mgr.set_ulim(args.queue, args.ulim)
    mgr.save(scheduler_fname)
    print('Set ulim of queue ' + args.queue + ' to ' + args.ulim)

    return reload_queues()


def queuestat(args):
    """ Prints a bunch of queue statistics. """
    parser = ArgumentParser(prog='queuestat',
                            description='HAdmin queuestat utility')
    parser.add_argument('queue', nargs='?', default='root')
    args = parser.parse_args(args)
    hxml = HXML.from_file(scheduler_fname)
    mgr = QueueManager(hxml)

    for queue in mgr.queue_list(args.queue):
        try:
            out = '\n'.join([
                queue,
                '\tcapacity:           ' + hxml[queue_cap_fqn(queue)],
                '\tmaximum capacity:   ' + hxml[queue_maxcap_fqn(queue)],
                '\tuser limit factor:  ' + hxml[queue_ulim_fqn(queue)],
                '\tstate:              ' + hxml[queue_state_fqn(queue)],
                '\tusers:              ' + hxml[queue_users_fqn(queue)],
                '\tadmins:             ' + hxml[queue_admins_fqn(queue)]
                ])
        except KeyError:
            out = queue + ' is not a leaf'

        print(out)


def sc(args):
    """ Runs a sanity check like a champion. """
    parser = ArgumentParser(prog='sc',
                            description='HAdmin sanity check utility')
    args = parser.parse_args(args)
    ret = 0

    mgr = QueueManager(HXML.from_file(scheduler_fname))

    for queue in mgr.sc_caps():
        ret = 1
        print('ERROR: The capacities of all subqueues of ' + queue +
              ' do not sum to 100')

    for queue in mgr.sc_maxcaps():
        ret = 1
        print('ERROR: The capacity of ' + queue +
              ' is greater than its maximum capacity')

    passwd_raw = open('/etc/passwd', 'r').read()
    for user in mgr.sc_users(passwd_raw):
        ret = 1
        print('WARN: User ' + user + ' does _not_ exist on this machine.')

    for admin in mgr.sc_admins(passwd_raw):
        ret = 1
        print('WARN: Admin ' + admin + ' does _not_ exist on this machine.')

    return ret


help_string = """Usage: hadmin <command> <command options>

Commands:
    queueadd    Add a queue
    queuecap    Change queue capacity
    queuedel    Remove a queue
    queueoff    Turn a queue off
    queueon     Turn a queue on
    queuestat   View queue information
    queueulim   Change queue user limit
    sc          Run a sanity check
    useradd     Add a user
    userdel     Remove a user"""

cmds = {
    'queueadd': queueadd,
    'queuecap': queuecap,
    'queuedel': queuedel,
    'queueoff': queueoff,
    'queueon': queueon,
    'queuestat': queuestat,
    'queueulim': queueulim,
    'sc': sc,
    'useradd': useradd,
    'userdel': userdel
    }


def run():
    if (len(sys.argv) <= 1 or sys.argv[1] == "-h"):
        print(help_string)
        return 0
    command = sys.argv[1]
    sysargs = sys.argv[2:]
    try:
        return cmds[command](sysargs)
    except KeyError:
        print(help_string)
    return 1
