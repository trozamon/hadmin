import hadmin.config as hconfig
import hadmin.auto as hauto
import argparse

# TODO Update to use the new config syntax

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
    added = False
    try:
        for conf_val in configs['mapred-queue-acls.xml'].configs:
            if conf_val.key == 'mapred.queue.' + args.queue + 'acl-submit-job':
                conf_val.value.append(',' + args.user)
                added = True
    except KeyError:
        print("Queue does not exist")
        return 1

    if added == True:
        print("Added " + args.user + " to queue " + args.queue)
    else:
        print("Not added for some reason")
