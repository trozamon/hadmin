"""
Entry points for the `hadmin` command.
"""


from argparse import ArgumentParser
from hadmin.conf import QueueGenerator
from hadmin.hdfs import NameNode, Directory
from hadmin.jmx import DataNodeJMX
import hadmin.rest
import hadmin.system
import os
import sys


def queuestat(args):
    """ Prints a bunch of queue statistics. """

    parser = ArgumentParser(prog='queuestat',
                            description='HAdmin queuestat utility')
    parser.add_argument('queue', nargs='?', default='root')
    args = parser.parse_args(args)

    try:
        mgr = hadmin.system.get_cap()

        for queue_name in mgr.queue_list(args.queue):
            queue = mgr.queue(queue_name)
            out = '\n'.join([
                queue_name,
                '\tcapacity:           ' + str(queue.cap_min),
                '\tmaximum capacity:   ' + str(queue.cap_max),
                '\tuser limit factor:  ' + str(queue.user_limit_factor),
                '\trunning:            ' + str(queue.running),
                '\tusers:              ' + str(queue.users),
                '\tadmins:             ' + str(queue.admins)
                ])

            print(out)
            print('')
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
        mgr = hadmin.system.get_cap()

        for queue in mgr.check_capacities():
            ret = 1
            print('ERROR: The capacities of all subqueues of ' + queue +
                  ' do not sum to 100')

        for queue in mgr.check_maximum_capacities():
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
    args = parser.parse_args(args)

    ret = 0

    jmx = DataNodeJMX()
    jmx.load_from_host(args.host)

    # Check for number of failed volumes
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
    args = parser.parse_args(args)

    ret = 0

    rest = hadmin.rest.NodeManager()
    rest.load_from_host(args.host)

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


def stats_nm(args):
    """
    Get some stats from a NodeManager
    """

    parser = ArgumentParser(prog='stats-nm', description='Get NM stats')
    parser.add_argument('host', nargs='?', default='localhost:8042')
    args = parser.parse_args(args)

    nm = hadmin.rest.NodeManager()
    nm.load_from_host(args.host, path=hadmin.rest.NM_INFO_PATH)

    print('Total Cores: ' + str(nm.allocated_cores))
    print('Total Memory: ' + str(nm.allocated_memory) + ' MB')

    return 0


def stats_rm(args):
    """
    Get some stats from a ResourceManager
    """

    rm = hadmin.system.get_rm()

    parser = ArgumentParser(prog='stats-rm', description='Get RM stats')
    parser.add_argument('host', nargs='?', default=rm.address)
    args = parser.parse_args(args)

    paths = [hadmin.rest.RM_METRICS_PATH, hadmin.rest.RM_SCHEDULER_PATH]
    rest_rm = hadmin.rest.ResourceManager.load_from_host(args.host,
                                                         paths=paths)

    print('Running Applications: ' + str(rest_rm.apps_running))

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

    mgr = hadmin.system.get_cap()
    if args.is_admin:
        mgr.queue(args.queue).admins.append(args.user)
        print("Added admin " + args.user + " to queue " + args.queue)
    else:
        mgr.queue(args.queue).users.append(args.user)
        print("Added user " + args.user + " to queue " + args.queue)

    hxml = mgr.to_hxml()
    hadmin.system.save_cap(hxml)

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

    mgr = hadmin.system.get_cap()
    if args.is_admin:
        mgr.queue(args.queue).admins.remove(args.user)
        print("Removed admin " + args.user + " from queue " +
              args.queue)
    else:
        mgr.queue(args.queue).users.remove(args.user)
        print("Removed user " + args.user + " from queue " +
              args.queue)

    hxml = mgr.to_hxml()
    hadmin.system.save_cap(hxml)

    return 0


def queueon(args):
    """ Turns a queue on. """

    parser = ArgumentParser(prog='queueon',
                            description='HAdmin queueon utility')
    parser.add_argument('queue')
    args = parser.parse_args(args)

    mgr = hadmin.system.get_cap()
    mgr.queue(args.queue).running = True

    hxml = mgr.to_hxml()
    hadmin.system.save_cap(hxml)

    print('Turned queue ' + args.queue + ' on')

    return 0


def queueoff(args):
    """ Turns a queue off. """

    parser = ArgumentParser(prog='queueoff',
                            description='HAdmin queueoff utility')
    parser.add_argument('queue')
    args = parser.parse_args(args)

    mgr = hadmin.system.get_cap()
    mgr.queue(args.queue).running = False

    hxml = mgr.to_hxml()
    hadmin.system.save_cap(hxml)

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

    mgr = hadmin.system.get_cap()

    if args.maxcap:
        mgr.queue(args.queue).cap_max = args.capacity
    else:
        mgr.queue(args.queue).cap_min = args.capacity

    hxml = mgr.to_hxml()
    hadmin.system.save_cap(hxml)

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

    mgr = hadmin.system.get_cap()

    mgr.queue(args.queue).user_limit_factor = args.ulim

    hxml = mgr.to_hxml()
    hadmin.system.save_cap(hxml)

    print('Set ulim of queue ' + args.queue + ' to ' + args.ulim)

    return 0


def genqueues(args):
    """ Generate capacity-scheduler.xml from YAML """

    parser = ArgumentParser(prog='genqueues',
                            description='HAdmin genqueues utility')

    parser.add_argument('conf_dir', help='Location of HAdmin YAML configs')

    default_output = os.path.join(hadmin.system.find_hxml_dir(),
                                  hadmin.system.CAPACITY_SCHEDULER_FILENAME)
    parser.add_argument('output', nargs='?', default=default_output,
                        help='Location of resulting capacity-scheduler.xml')

    args = parser.parse_args(args)

    sched = hadmin.system.get_cap()
    gen = QueueGenerator.load_dir(args.conf_dir)
    new_root_queue = gen.generate()

    if new_root_queue is None:
        print("Your YAML stuff is invalid")
        return 1

    sched.root_queue = new_root_queue
    hxml = sched.to_hxml()
    hxml.save(args.output)

    print("Generated some queues and stuff into " + args.output)

    return 0


def fhs(args):
    """ Check and optionally fixup proper HDFS directory permissions """

    parser = ArgumentParser(prog='fhs',
                            description='HAdmin FHS utility')

    parser.add_argument('--fixup', dest='fixup', action='store_const',
                        const=True, default=False,
                        help='Fixup permissions that are not correct')

    args = parser.parse_args(args)

    user_dirs = []

    sched = hadmin.system.get_cap()
    for q in sched.queue_list():
        user_dirs += filter(lambda thing: thing is not None,
                            sched.queue(q).users)

    user_dirs = list(map(lambda u: Directory.from_username(u), set(user_dirs)))

    for d in NameNode.FHS_DIRS + user_dirs:
        if not d:
            continue

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
    genqueues   Generate CapacityScheduler queues from YAML files
    queuecap    Change queue capacity
    queueoff    Turn a queue off
    queueon     Turn a queue on
    queuestat   View queue information
    queueulim   Change queue user limit
    sc          Run a sanity check
    stats-nm    Get some statistics about a YARN NodeManager
    stats-rm    Get some statistics about a YARN ResourceManager
    useradd     Add a user
    userdel     Remove a user"""

cmds = {
    'chk-dn': chk_dn,
    'chk-nm': chk_nm,
    'fhs': fhs,
    'genqueues': genqueues,
    'queuecap': queuecap,
    'queueoff': queueoff,
    'queueon': queueon,
    'queuestat': queuestat,
    'queueulim': queueulim,
    'sc': sc,
    'stats-nm': stats_nm,
    'stats-rm': stats_rm,
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
