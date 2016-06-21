"""
Entry points for the `hadmin` command.
"""


from argparse import ArgumentParser
import sys
from hadmin.hdfs import NameNode, Directory
from hadmin.jmx import DataNodeJMX
from hadmin.rest import NodeManagerREST
from hadmin.system import get_system_capacity_scheduler
from hadmin.yarn import CapacityScheduler


def queuestat(args):
    """ Prints a bunch of queue statistics. """

    parser = ArgumentParser(prog='queuestat',
                            description='HAdmin queuestat utility')
    parser.add_argument('queue', nargs='?', default='root')
    args = parser.parse_args(args)

    try:
        mgr = get_system_capacity_scheduler()

        for queue_name in mgr.queue_list(args.queue):
            queue = mgr.queue(queue_name)
            out = '\n'.join([
                queue_name,
                '\tcapacity:           ' + str(queue.cap_min),
                '\tmaximum capacity:   ' + 'None',
                '\tuser limit factor:  ' + 'None',
                '\tstate:              ' + 'None',
                '\tusers:              ' + 'None',
                '\tadmins:             ' + 'None'
                ])

            print(out)
    except KeyError:
        print("Your CapacityScheduler configuration is malformed")
        return 1

    return 0


def sc(args):
    """ Runs a sanity check like a champion. """

    parser = ArgumentParser(prog='sc',
                            description='HAdmin sanity check utility')
    args = parser.parse_args(args)
    ret = 0

    try:
        mgr = get_system_capacity_scheduler()

        for queue in mgr.sc_caps():
            ret = 1
            print('ERROR: The capacities of all subqueues of ' + queue +
                  ' do not sum to 100')

        for queue in mgr.sc_maxcaps():
            ret = 1
            print('ERROR: The capacity of ' + queue +
                  ' is greater than its maximum capacity')
    except KeyError:
        print("your CapacityScheduler configuration is malformed")
        ret = 1

    return ret


def chk_dn(args):
    """ Checks various datanode-related health things. """

    parser = ArgumentParser(prog='chk-dn',
                            description='Check datanode stats/health')
    parser.add_argument('host', nargs='?', default='localhost:50075')
    parser.add_argument('--failed-volumes', dest='failed_volumes',
                        action='store_const', const=True, default=False,
                        help='Check the number of failed volumes')
    args = parser.parse_args(args)

    ret = 0

    jmx = DataNodeJMX()
    jmx.load_from_host(args.host)

    if args.failed_volumes:
        nfails = jmx.getFailedVolumes()
        msg = ' volumes have failed'

        if nfails == 1:
            msg = ' volume has failed'

        print(str(nfails) + msg)

        if nfails > 0:
            ret = 1

    return ret


def chk_nm(args):
    """ Checks various nodemanager-related things """

    parser = ArgumentParser(prog='chk-nm',
                            description='Check nodemanager stats/health')
    parser.add_argument('host', nargs='?', default='localhost:8042')
    parser.add_argument('--health', dest='health', action='store_const',
                        const=True, default=False,
                        help='Check the node health')
    args = parser.parse_args(args)

    ret = 0

    rest = NodeManagerREST()
    rest.load_from_host(args.host)

    if args.health:
        status = rest.isHealthy()

        if status:
            msg = 'node is healthy'
        else:
            msg = 'node is unhealthy'
            ret = 1

        if len(rest.getHealthReport()) > 0:
            msg = msg + ': ' + rest.getHealthReport()

        print(msg)

    return ret


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

    mgr = CapacityScheduler()
    if args.is_admin:
        mgr.add_admin(args.user, args.queue)
        print("Added admin " + args.user + " to queue " + args.queue)
    else:
        mgr.add_user(args.user, args.queue)
        print("Added user " + args.user + " to queue " + args.queue)

    mgr.save()

    nn = NameNode()
    for u in args.user.split(','):
        nn.add_user_hdfs(u)

    return 0


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

    mgr = CapacityScheduler()
    if args.is_admin:
        mgr.del_admin(args.user, args.queue)
        print("Removed admin " + args.user + " from queue " +
              args.queue)
    else:
        mgr.del_user(args.user, args.queue)
        print("Removed user " + args.user + " from queue " +
              args.queue)

    mgr.save()

    return 0


def queueadd(args):
    """ Adds a queue. """

    parser = ArgumentParser(prog='queueadd',
                            description='HAdmin queueadd utility')
    parser.add_argument('queue')
    parser.add_argument('user')
    args = parser.parse_args(args)

    mgr = CapacityScheduler()

    try:
        input = raw_input
    except NameError:
        pass

    confirm = input(' '.join(['Are you sure you want to add queue',
                    args.queue, 'with initial user/admin', args.user + '?',
                    '[y/N] ']))

    if confirm != 'y' and confirm != 'Y':
        return 0

    mgr.add(args.queue, args.user)
    print('Added queue ' + args.queue +
          ' with initial user/admin ' + args.user)

    mgr.save()

    nn = NameNode()
    nn.add_user_hdfs(args.user)

    return 0


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
        mgr = CapacityScheduler()
        mgr.delete(args.queue)
        mgr.save()
        print('Removed queue ' + args.queue)
        return 0

    return queueoff([args.queue])


def queueon(args):
    """ Turns a queue on. """

    parser = ArgumentParser(prog='queueon',
                            description='HAdmin queueon utility')
    parser.add_argument('queue')
    args = parser.parse_args(args)

    mgr = CapacityScheduler()
    mgr.on(args.queue)
    mgr.save()

    print('Turned queue ' + args.queue + ' on')

    return 0


def queueoff(args):
    """ Turns a queue off. """

    parser = ArgumentParser(prog='queueoff',
                            description='HAdmin queueoff utility')
    parser.add_argument('queue')
    args = parser.parse_args(args)

    mgr = CapacityScheduler()
    mgr.off(args.queue)
    mgr.save()

    print('Turned queue ' + args.queue + ' off')

    return 0


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

    mgr = CapacityScheduler()
    if args.maxcap:
        mgr.set_maxcap(args.queue, args.capacity)
    else:
        mgr.set_cap(args.queue, args.capacity)
    mgr.save()

    out = 'Set '
    if args.maxcap:
        out = out + 'maximum '
    out = out + 'capacity of queue ' + args.queue + ' to ' + args.capacity
    print(out)

    return 0


def queueulim(args):
    """ Sets a queue's user limit factor. """

    parser = ArgumentParser(prog='queueulim',
                            description='HAdmin queueulim utility')
    parser.add_argument('queue')
    parser.add_argument('ulim')
    args = parser.parse_args(args)

    mgr = CapacityScheduler()
    mgr.set_ulim(args.queue, args.ulim)
    mgr.save()

    print('Set ulim of queue ' + args.queue + ' to ' + args.ulim)

    return 0


def fhs(args):
    """ Check and optionally fixup proper HDFS directory permissions """

    parser = ArgumentParser(prog='fhs',
                            description='HAdmin FHS utility')

    parser.add_argument('--fixup', dest='fixup', action='store_const',
                        const=True, default=False,
                        help='Fixup permissions that are not correct')

    args = parser.parse_args(args)

    for d in NameNode.FHS_DIRS:
        try:
            current = Directory.from_hdfs(d.path)

            if d != current:
                print(current.path + ' has problems:')

                if d.perms != current.perms:
                    print(' * perms are ' + current.perms +
                          ', but should be ' + d.perms)

                if d.owner != current.owner:
                    print(' * owner is ' + current.owner +
                          ', but should be ' + d.owner)

                if d.group != current.group:
                    print(' * group is ' + current.group +
                          ', but should be ' + d.group)

                if args.fixup:
                    d.write()
        except IOError:
            print(d.path + ' does not exist')

            if args.fixup:
                d.write()


help_string = """Usage: hadmin <command> <command options>

Commands:
    chk-dn      Check datanode health
    chk-nm      Check nodemanager health
    fhs         Check and fix problems with standard HDFS directories
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
    'chk-dn': chk_dn,
    'chk-nm': chk_nm,
    'fhs': fhs,
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
    cmd_func = None

    try:
        cmd_func = cmds[command]
    except KeyError:
        print(help_string)
        return 1

    return cmd_func(sysargs)
