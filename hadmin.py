from argparse import ArgumentParser
import xml.etree.ElementTree as ET
import pkgutil
import os


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
        ret = cls(ET.parse(fname))
        return ret

    def __getitem__(self, prop):
        for node in self.tree.findall('property'):
            if node.find('name').text == prop:
                return node.find('value').text

        raise KeyError('Key ' + prop + ' not found')

    def __setitem__(self, prop, val):
        success = False
        for node in self.tree.findall('property'):
            if node.find('name').text == prop:
                node.find('value').text = val
                success = True

        if success is not True:
            el = ET.Element('property')
            el.append(ET.Element('name'))
            el.append(ET.Element('value'))
            el.find('name').text = prop
            el.find('value').text = val
            self.tree.append(el)

    def remove(self, prop):
        for node in self.tree.findall('property'):
            if node.find('name').text == prop:
                self.tree.remove(node)

    def save(self, fname):
        self.tree.write(fname)


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
        self.hxml[queue_cap_fqn(queue)] = cap

    def set_maxcap(self, queue, maxcap):
        self.hxml[queue_maxcap_fqn(queue)] = maxcap

    def set_ulim(self, queue, ulim):
        self.hxml[queue_ulim_fqn(queue)] = ulim

    def set_state(self, queue, state):
        self.hxml[queue_state_fqn(queue)] = state

    def add(self, queue, user):
        parent = queue_parent(queue)
        self.__insert(queue_subs_fqn(parent), queue)

        self.add_user(user, queue)
        self.add_admin(user, queue)
        self.set_cap(queue, '0')
        self.set_maxcap(queue, '100')
        self.set_ulim(queue, '25')
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
    with hconfig.Internal.from_dir('.') as mgr:
        try:
            if args.is_admin:
                mgr.add_admin(args.user, args.queue)
                print("Added admin " + args.user + " to queue " + args.queue)
            else:
                mgr.add_user(args.user, args.queue)
                print("Added user " + args.user + " to queue " + args.queue)
        except KeyError as e:
            print(str(e)[1:-1])


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
    with hconfig.Internal.from_dir('.') as mgr:
        try:
            if args.is_admin:
                mgr.del_admin(args.user, args.queue)
                print("Removed admin " + args.user + " from queue " +
                      args.queue)
            else:
                mgr.del_user(args.user, args.queue)
                print("Removed user " + args.user + " from queue " +
                      args.queue)
        except ValueError as e:
            print(str(e)[1:-1])
        except AttributeError as e:
            print(e)


def queueadd(args):
    parser = ArgumentParser(prog='queueadd',
                            description='HAdmin queueadd utility')
    parser.add_argument('queue')
    parser.add_argument('user')
    args = parser.parse_args(args)
    with hconfig.Internal.from_dir('.') as mgr:
        try:
            mgr.add_queue(args.queue, args.user)
            print('Added queue ' + args.queue +
                  ' with initial user/admin ' + args.user)
        except KeyError as e:
            print(str(e)[1:-1])


def queuedel(args):
    parser = ArgumentParser(prog='queueadd',
                            description='HAdmin queueadd utility')
    parser.add_argument('queue')
    args = parser.parse_args(args)
    with hconfig.Internal.from_dir('.') as mgr:
        try:
            mgr.del_queue(args.queue)
            print('Removed queue ' + args.queue)
        except KeyError as e:
            print(str(e)[1:-1])
