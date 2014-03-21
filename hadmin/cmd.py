import hadmin.hconfig as hconfig
import hadmin.auto as hauto
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
    configs = hauto.create_configs()
    try:
        configs['mapred-queue-acls.xml'].configs['mapred.queue.' + args.queue + 'acl-submit-job'].append(',' + args.user)
    except KeyError:
        print("Queue does not exist")
        return 1
    print("Trying to add " + args.user + " to queue " + args.queue)
