import hadmin.config as hconfig
import argparse

def useradd(args):
    """
    Takes in the args that come after 'useradd' on the command line and also
    and array of config files so it can make changes

    """

    parser = argparse.ArgumentParser(prog='useradd',
            description='HAdmin useradd utility')
    parser.add_argument('user')
    parser.add_argument('queue')
    parser.add_argument('--admin', dest='is_admin', action='store_const',
            const=True, default=False,
            help='Add an administrator')
    args = parser.parse_args(args)
    with hconfig.HadminManager('.') as mgr:
        try:
            if args.is_admin:
                mgr.add_admin(args.user, args.queue)
                print("Added user " + args.user + " to queue " + args.queue)
            else:
                mgr.add_user(args.user, args.queue)
                print("Added admin " + args.user + " to queue " + args.queue)
        except KeyError as e:
            print(str(e)[1:-1])

def userdel(args):
    """
    Takes in a user and removes him from a queue

    """

    parser = argparse.ArgumentParser(prog='userdel',
            description='HAdmin userdel utility')
    parser.add_argument('user')
    parser.add_argument('queue')
    parser.add_argument('--admin', dest='is_admin', action='store_const',
            const=True, default=False,
            help='Delete an administrator')
    args = parser.parse_args(args)
    with hconfig.HadminManager('.') as mgr:
        try:
            if args.is_admin:
                mgr.del_admin(args.user, args.queue)
                print("Removed admin " + args.user + " from queue " + args.queue)
            else:
                mgr.del_user(args.user, args.queue)
                print("Removed user " + args.user + " from queue " + args.queue)
        except ValueError as e:
            print(str(e)[1:-1])
        except AttributeError as e:
            print(e)

def queueadd(args):
    parser = argparse.ArgumentParser(prog='queueadd',
            description='HAdmin queueadd utility')
    parser.add_argument('queue')
    parser.add_argument('user')
    args = parser.parse_args(args)
    with hconfig.HadminManager('.') as mgr:
        try:
            mgr.add_queue(args.queue, args.user)
            print('Added queue ' + args.queue + ' with initial user/admin ' + args.user)
        except KeyError as e:
            print(str(e)[1:-1])

def queuedel(args):
    parser = argparse.ArgumentParser(prog='queueadd',
            description='HAdmin queueadd utility')
    parser.add_argument('queue')
    args = parser.parse_args(args)
    with hconfig.HadminManager('.') as mgr:
        try:
            mgr.del_queue(args.queue)
            print('Removed queue ' + args.queue)
        except KeyError as e:
            print(str(e)[1:-1])

def queuemod(args):
    parser = argparse.ArgumentParser(prog='queuemod',
            description='HAdmin queuemod utility')
    parser.add_argument('queue')
    parser.add_argument('--capacity', nargs=1, help='New capacity')
    parser.add_argument('--maxcap', nargs=1, help='New maximum capacity')
    parser.add_argument('--tpu', nargs=1, help='New maximum tasks per user')
    args = parser.parse_args(args)
    with hconfig.HadminManager('.') as mgr:
        try:
            if args.capacity is not None:
                tmp = args.capacity[0]
                mgr.set_queue_cap(args.queue, tmp)
                print('Set ' + args.queue + ' capacity to ' + tmp)
            if args.maxcap is not None:
                tmp = args.maxcap[0]
                mgr.set_queue_max_cap(args.queue, tmp)
                print('Set ' + args.queue + ' maximum capacity to ' + tmp)
            if args.tpu is not None:
                tmp = args.tpu[0]
                mgr.set_queue_max_init_tpu(args.queue, tmp)
                print('Set ' + args.queue + ' tasks per user to ' + tmp)
        except TypeError as e:
            print(e)
