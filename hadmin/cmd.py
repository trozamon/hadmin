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
    args = parser.parse_args(args)
    with hconfig.HadminManager('.') as mgr:
        mgr.add_user(args.user, args.queue)

    print("Added " + args.user + " to queue " + args.queue)

def userdel(args):
    print("Coming soon")

def queueadd(args):
    print("Coming soon")

def queuedel(args):
    print("Coming soon")

def queuemod(args):
    print("Coming soon")

modules = {
        'useradd' : useradd,
        'userdel' : userdel,
        'queueadd' : queueadd,
        'queuedel' : queuedel,
        'queuemod' : queuemod
        }
