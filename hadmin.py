from argparse import ArgumentParser
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


def queue_fqn(queue):
    if (queue[0:5] == 'root.'):
        return queue
    return ''.join(['root.', queue])


def queue_parent(queue):
    return '.'.join(queue_fqn(queue).split('.')[0:-1])

def queue_cap_fqn(queue):
    return '.'.join([pre_scheduler, queue_fqn(queue), post_cap])

def queue_maxcap_fqn(queue):
    return '.'.join([pre_scheduler, queue_fqn(queue), post_maxcap])

def queue_users_fqn(queue):
    return '.'.join([pre_scheduler, queue_fqn(queue), post_users])

def queue_admins_fqn(queue):
    return '.'.join([pre_scheduler, queue_fqn(queue), post_admins])

def queue_state_fqn(queue):
    return '.'.join([pre_scheduler, queue_fqn(queue), post_state])

def queue_ulim_fqn(queue):
    return '.'.join([pre_scheduler, queue_fqn(queue), post_ulim])

def queue_subs_fqn(queue):
    return '.'.join([pre_scheduler, queue_fqn(queue), post_subs])


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
