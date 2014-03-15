import modules.hconfig
import argparse

def useradd(args, configs):
    """
    Takes in the args that come after 'useradd' on the command line and also
    and array of config files so it can make changes

    """

    parser = argparse.ArgumentParser(prog='useradd',
            description='HAdmin useradd utility')
    parser.add_argument('user')
    parser.add_argument('queue')
    args = parser.parse_args(args)
    if configs == None:
        print("Trying to add " + args.user + " to queue " + args.queue)
    else:
        print("Not supported yet")
