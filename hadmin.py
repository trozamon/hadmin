from argparse import ArgumentParser
import xml.etree.ElementTree as ET
import pkgutil
import os


scheduler_fname = 'capacity-scheduler.xml'
pre_scheduler = 'yarn.scheduler.capacity'
post_users = 'acl_submit_applications'
post_admins = 'acl_administer_queue'
post_cap = 'capacity'
post_maxcap = 'maximum-capacity'
post_state = 'state'
post_ulim = 'user-limit-factor'
post_subs = 'queues'


def queue_fqn(queue='root'):
    if (queue[0:4] == 'root'):
        return queue
    return ''.join(['root.', queue])

def queue_parent(queue='root'):
    return '.'.join(queue_fqn(queue).split('.')[0:-1])

def queue_cap_fqn(queue='root'):
    return '.'.join([pre_scheduler, queue_fqn(queue), post_cap])

def queue_maxcap_fqn(queue='root'):
    return '.'.join([pre_scheduler, queue_fqn(queue), post_maxcap])

def queue_users_fqn(queue='root'):
    return '.'.join([pre_scheduler, queue_fqn(queue), post_users])

def queue_admins_fqn(queue='root'):
    return '.'.join([pre_scheduler, queue_fqn(queue), post_admins])

def queue_state_fqn(queue='root'):
    return '.'.join([pre_scheduler, queue_fqn(queue), post_state])

def queue_ulim_fqn(queue='root'):
    return '.'.join([pre_scheduler, queue_fqn(queue), post_ulim])

def queue_subs_fqn(queue='root'):
    return '.'.join([pre_scheduler, queue_fqn(queue), post_subs])


class HXML:
    def __init__(self, etree):
        self.tree = etree

    @classmethod
    def from_etree(cls, etree):
        ret = cls(etree)
        return ret

    @classmethod
    def from_str(cls, string):
        ret = cls(ET.fromstring(string))
        return ret

    @classmethod
    def from_file(cls, fname):
        ret = cls(ET.parse(fname).getroot())
        return ret

    def __getitem__(self, prop):
        for node in self.tree.findall('property'):
            if node.find('name').text == prop:
                return node.find('value').text

        raise KeyError('Key ' + prop + ' not found')

    def __setitem__(self, prop, val):
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
        for node in self.tree.findall('property'):
            if node.find('name').text == prop:
                self.tree.remove(node)

    def keys(self):
        ret = list()
        for node in self.tree.findall('property'):
            ret.append(node.find('name').text)
        return sorted(ret)

    def save(self, fname):
        ET.ElementTree(self.tree).write(fname)


class QueueManager:

    def __init__(self, hxml):
        self.hxml = hxml

    def __insert(self, prop, value):
        csv = [value]
        try:
            csv = self.hxml[prop].split(',')
            if value not in csv:
                csv.append(value)
        except KeyError:
            pass

        self.hxml[prop] = ','.join(sorted(csv))

    def __unsert(self, prop, value):
        csv = self.hxml[prop].split(',')

        if len(csv) == 1:
            raise ValueError("Can't delete last element in " + prop)

        for i in range(len(csv)):
            if csv[i] == value:
                del(csv[i])
                break

        self.hxml[prop] = ','.join(sorted(csv))

    def add_user(self, user, queue):
        self.__insert(queue_users_fqn(queue), user)

    def add_admin(self, user, queue):
        self.__insert(queue_admins_fqn(queue), user)

    def set_cap(self, queue, cap):
        cap = int(cap)
        if cap > 100 or cap < 0:
            raise ValueError('Queue capacity must be between 0 and 100')

        self.hxml[queue_cap_fqn(queue)] = cap

    def set_maxcap(self, queue, maxcap):
        maxcap = int(maxcap)
        if maxcap > 100 or maxcap < 0:
            raise ValueError(
                    'Queue maximum capacity must be between 0 and 100'
                    )

        self.hxml[queue_maxcap_fqn(queue)] = maxcap

    def set_ulim(self, queue, ulim):
        ulim = float(ulim)

        if ulim > 1.0 or ulim < 0.0:
            raise ValueError('ulim must be a float between 0 and 1')

        self.hxml[queue_ulim_fqn(queue)] = ulim

    def set_state(self, queue, state):
        self.hxml[queue_state_fqn(queue)] = state

    def off(self, queue):
        self.set_state(queue, 'stopped')

    def on(self, queue):
        self.set_state(queue, 'running')

    def add(self, queue, user):
        parent = queue_parent(queue)
        self.__insert(queue_subs_fqn(parent), queue.split('.')[-1])

        self.add_user(user, queue)
        self.add_admin(user, queue)
        self.set_cap(queue, '0')
        self.set_maxcap(queue, '100')
        self.set_ulim(queue, '0.25')
        self.set_state(queue, 'running')

    def delete(self, queue):
        parent = queue_parent(queue)
        self.__unsert(queue_subs_fqn(parent), queue)
        self.hxml.remove(queue_users_fqn(queue))
        self.hxml.remove(queue_admins_fqn(queue))
        self.hxml.remove(queue_cap_fqn(queue))
        self.hxml.remove(queue_maxcap_fqn(queue))
        self.hxml.remove(queue_ulim_fqn(queue))
        self.hxml.remove(queue_state_fqn(queue))

    def del_user(self, user, queue):
        self.__unsert(queue_users_fqn(queue), user)

    def del_admin(self, admin, queue):
        self.__unsert(queue_admins_fqn(queue), admin)

    def save(self, fname):
        self.hxml.save(fname)

    def queue_list(self, queue='root'):
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


def useradd(args):
    """
    Takes in the args that come after 'useradd' on the command line and also
    and array of config files so it can make changes

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

    return 0

def userdel(args):
    """
    Takes in a user and removes him from a queue

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


def queueadd(args):
    parser = ArgumentParser(prog='queueadd',
                            description='HAdmin queueadd utility')
    parser.add_argument('queue')
    parser.add_argument('user')
    args = parser.parse_args(args)

    mgr = QueueManager(HXML.from_file(scheduler_fname))
    mgr.add(args.queue, args.user)
    print('Added queue ' + args.queue +
          ' with initial user/admin ' + args.user)

    mgr.save(scheduler_fname)


def queuedel(args):
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
    else:
        queueon(args.queue)

def queueon(args):
    parser = ArgumentParser(prog='queueon',
                            description='HAdmin queueon utility')
    parser.add_argument('queue')
    args = parser.parse_args(args)
    mgr = QueueManager(HXML.from_file(scheduler_fname))
    mgr.on(args.queue)
    mgr.save(scheduler_fname)
    print('Turned queue ' + args.queue + 'on')

def queueoff(args):
    parser = ArgumentParser(prog='queueon',
                            description='HAdmin queueon utility')
    parser.add_argument('queue')
    args = parser.parse_args(args)
    mgr = QueueManager(HXML.from_file(scheduler_fname))
    mgr.off(args.queue)
    mgr.save(scheduler_fname)
    print('Turned queue ' + args.queue + 'off')
