"""
YARN-related library functionality
----------------------------------

hadmin.yarn allows the managing of scheduling queues and provides a library
interface for the metrics REST APIs of the ResourceManager and NodeManager.

Files handled:

* capacity-scheduler.xml
* yarn-site.xml
"""


import subprocess
from hadmin.util import HXML


class Queue(object):
    """
    An abstraction of a queue

    Queues are fundamental to managing a YARN cluster. Each queue has a list
    of allowed users, a minimum guaranteed capacity, and maximum capacity
    (among other attributes).

    :py:class:`Queue` swallows '*' in the ACLs for admins and users, replacing
    with ' '. Allowing any user to submit to a queue is terrible practice.
    """

    pre_scheduler = 'yarn.scheduler.capacity'
    post_users = 'acl_submit_applications'
    post_admins = 'acl_administer_queue'
    post_cap = 'capacity'
    post_maxcap = 'maximum-capacity'
    post_state = 'state'
    post_ulim = 'user-limit-factor'
    post_subs = 'queues'

    DEFAULT_ADMIN_LIST = []
    DEFAULT_CAP = 100.0
    DEFAULT_MAXCAP = 100.0
    DEFAULT_RUNNING = True
    DEFAULT_ULIM = 1.0
    DEFAULT_USER_LIST = []

    def __init__(self, name=None, admins=DEFAULT_ADMIN_LIST,
                 users=DEFAULT_USER_LIST, running=DEFAULT_RUNNING):
        if name is None:
            raise KeyError('Queues must be named')

        self.name = name
        self.admins = admins
        self.users = users
        self.running = running
        self.user_limit_factor = Queue.DEFAULT_ULIM
        self.cap_max = Queue.DEFAULT_MAXCAP
        self.cap_min = Queue.DEFAULT_CAP
        self.subqueues = []

    @classmethod
    def fqn_users(cls, queue_name):
        """ Transform a queue name into the property name for its users """

        return '.'.join([cls.pre_scheduler, queue_name, cls.post_users])

    @classmethod
    def fqn_admins(cls, queue_name):
        """ Transform a queue name into the property name for its admins """

        return '.'.join([cls.pre_scheduler, queue_name, cls.post_admins])

    @classmethod
    def fqn_cap(cls, queue_name):
        """ Transform a queue name into the property name for its capacity """

        return '.'.join([cls.pre_scheduler, queue_name, cls.post_cap])

    @classmethod
    def fqn_maxcap(cls, queue_name):
        """
        Transform a queue name into the property name for its maximum capacity
        """

        return '.'.join([cls.pre_scheduler, queue_name, cls.post_maxcap])

    @classmethod
    def fqn_state(cls, queue_name):
        """ Transform a queue name into the property name for its state """

        return '.'.join([cls.pre_scheduler, queue_name, cls.post_state])

    @classmethod
    def fqn_ulim(cls, queue_name):
        """
        Transform a queue name into the property name for its user limit factor
        """

        return '.'.join([cls.pre_scheduler, queue_name, cls.post_ulim])

    @classmethod
    def fqn_subs(cls, queue_name):
        """
        Transform a queue name into the property name for its subqueue list
        """

        return '.'.join([cls.pre_scheduler, queue_name, cls.post_subs])

    @classmethod
    def from_hxml(cls, hxml, fqn):
        """
        Create a Queue from part of an HXML

        Given the fqn (fully qualified name) of the queue, read its attributes
        into a Queue and return it
        """

        adms = Queue.DEFAULT_ADMIN_LIST
        users = Queue.DEFAULT_USER_LIST

        try:
            adms = hxml[cls.fqn_admins(fqn)].split(',')
            users = hxml[cls.fqn_users(fqn)].split(',')

            if '*' in adms:
                adms.remove('*')

            if '*' in users:
                users.remove('*')

        except KeyError:
            adms = Queue.DEFAULT_ADMIN_LIST
            pass

        cap = float(hxml.get_or(cls.fqn_cap(fqn), Queue.DEFAULT_CAP))
        maxcap = float(hxml.get_or(cls.fqn_maxcap(fqn), Queue.DEFAULT_MAXCAP))
        ulim = float(hxml.get_or(cls.fqn_ulim(fqn), Queue.DEFAULT_ULIM))

        subs = []
        try:
            subs = hxml[cls.fqn_subs(fqn)]

            if subs is None:
                subs = []
            else:
                subs = subs.split(',')

        except KeyError:
            pass

        running = True
        if hxml.get_or(cls.fqn_state(fqn), 'RUNNING') == 'STOPPED':
            running = False

        q = cls(name=fqn.split('.')[-1], admins=adms, users=users,
                running=running)
        q.cap_max = maxcap
        q.cap_min = cap
        q.ulim = ulim

        for sub in subs:
            q.subqueues.append(Queue.from_hxml(hxml, '.'.join([fqn, sub])))

        return q

    def subqueue(self, name):
        """
        Retrieve a subqueue of this queue
        """

        for q in self.subqueues:
            if q.name == name:
                return q

        return None

    def check_capacities(self, fqn_prefix=None):
        """
        Check that this queue's subqueues have capacities that add to 100,
        and perform this operation recursively on all subqueues.
        """

        if len(self.subqueues) == 0:
            return []

        fqn = self.get_fqn(fqn_prefix)
        failures = []
        cap = 0

        for q in self.subqueues:
            cap += q.cap_min
            failures += q.check_capacities()

        if cap != 100.0:
            failures.append(fqn)

        return failures

    def check_maximum_capacities(self, fqn_prefix=None):
        """
        Check that this queue's capacity is less than or equal to its capacity,
        and perform this test recursively on all subqueues.
        """

        failures = []

        if self.cap_max < self.cap_min:
            failures.append(self.get_fqn(fqn_prefix))

        for q in self.subqueues:
            failures += q.check_maximum_capacities(self.get_fqn(fqn_prefix))

        return failures

    def get_fqn(self, prefix=None):
        """
        Get the fqn (fully qualified name) of this queue given a prefix
        """

        fqn = self.name

        if prefix is not None:
            fqn = '.'.join([prefix, self.name])

        return fqn

    def to_hxml(self, fqn_prefix=None):
        """
        Convert this Queue to a blurb of HXML
        """

        fqn = self.get_fqn(fqn_prefix)

        tmp = HXML()

        admin_list = ','.join(sorted(set(self.admins)))
        if not self.admins:
            admin_list = ' '

        user_list = ','.join(sorted(set(self.users)))
        if not self.users:
            user_list = ' '

        tmp[Queue.fqn_admins(fqn)] = admin_list
        tmp[Queue.fqn_users(fqn)] = user_list
        tmp[Queue.fqn_cap(fqn)] = str(self.cap_min)
        tmp[Queue.fqn_maxcap(fqn)] = str(self.cap_max)
        tmp[Queue.fqn_state(fqn)] = self.get_state_str()
        tmp[Queue.fqn_subs(fqn)] = ','.join(
                sorted(map(lambda q: q.name, self.subqueues)))
        tmp[Queue.fqn_ulim(fqn)] = str(self.user_limit_factor)

        for q in self.subqueues:
            tmp.merge(q.to_hxml(fqn))

        return tmp

    def get_state_str(self):
        """
        Get the state string of this queue. Either RUNNING or STOPPED
        """

        if self.running:
            return 'RUNNING'

        return 'STOPPED'

    @property
    def cap_min(self):
        """
        This Queue's minimum capacity
        """

        return self._cap_min

    @cap_min.setter
    def cap_min(self, new_cap_min):
        """
        Set the minimum capacity.

        Checks to ensure that the new capacity is a valid value, and raises
        ValueError if it is not
        """

        tmp = float(new_cap_min)
        if 0.0 <= tmp <= 100.0:
            self._cap_min = tmp
        else:
            raise ValueError("cap_min must be between 0 and 100")

    @property
    def cap_max(self):
        """
        Get the maximum capacity of this queue
        """

        return self._cap_max

    @cap_max.setter
    def cap_max(self, new_cap_max):
        """
        Set the maximum capacity

        Checks to ensure the new capacity is valid. Raises ValueError if the
        new value is not valid
        """

        tmp = float(new_cap_max)
        if 0.0 <= tmp <= 100.0:
            self._cap_max = tmp
        else:
            raise ValueError("cap_max must be between 0 and 100")

    @property
    def user_limit_factor(self):
        """
        The queue's user limit factor
        """

        return self._ulim

    @user_limit_factor.setter
    def user_limit_factor(self, new_val):
        """
        Set the queue's user limit factor

        Checks to ensure validity of the new value. Raises ValueError if the
        new value is not valid
        """

        tmp = float(new_val)
        if tmp < 0.0:
            raise ValueError("cap_max must be between 0 and 100")

        self._ulim = tmp

    @property
    def users(self):
        """
        List of queue users
        """

        if not self._users:
            if self.subqueues:
                return ['*']
            else:
                return [' ']

        return self._users

    @users.setter
    def users(self, new_val):
        """
        Set the list of users
        """

        self._users = new_val

    @property
    def admins(self):
        """
        List of queue admins
        """

        if not self._admins:
            if self.subqueues:
                return ['*']
            else:
                return [' ']

        return self._admins

    @admins.setter
    def admins(self, new_val):
        """
        Set the list of admins
        """

        self._admins = new_val


class CapacityScheduler:
    """
    A class to specifically manage Hadoop's CapacityScheduler.

    Initialize the CapacityScheduler with an instance of :py:class:HXML.

    CapacityScheduler initializes its internal state with the given HXML.
    However, it does not modify the given HXML at all, and any
    modifications require calling to_hxml() to obtain a current
    configuration in Hadoop XML format.
    """

    def __init__(self, hxml):
        self.root_queue = Queue(name='root')

        if hxml is not None:
            self.root_queue = Queue.from_hxml(hxml, 'root')

    @classmethod
    def from_file(cls, fname):
        """
        Load a CapacityScheduler from a file
        """

        return cls(HXML.from_file(fname))

    def to_hxml(self):
        """
        Generate an HXML that is the configuration of this CapacityScheduler
        """

        return self.root_queue.to_hxml()

    def queue(self, fqn):
        """
        Get a queue
        """

        parts = fqn.split('.')
        q = self.root_queue

        if parts[0] == 'root':
            del parts[0]

        for part in parts:
            q = q.subqueue(part)
            if q is None:
                return None

        return q

    def queue_list(self, queue='root'):
        """ Generates and returns a sorted list of queues """

        root = self.queue(queue)
        n = [queue]

        def recursive_list(queue, pre=None):
            names = list(map(lambda q: q.get_fqn(pre), queue.subqueues))

            for q in queue.subqueues:
                names += recursive_list(q, q.get_fqn(pre))

            return names

        n += recursive_list(root, queue)

        return sorted(n)

    def check_capacities(self):
        """
        Sanity check for the queue capacities. Checks that all queues on the
        same level have capacities that add to 100.

        Returns a list of queues whose subqueues capacities fail to sum to
        100.0
        """

        return sorted(self.root_queue.check_capacities())

    def check_maximum_capacities(self):
        """
        Sanity check for the queue maximum capacities. Ensures that each
        queue's maximum capacity is greater than or equal to its capacity.

        Returns a list of queues that have a maximum capacity lower than
        their guaranteed capacity.
        """

        return sorted(self.root_queue.check_maximum_capacities())


class ResourceManager:

    def __init__(self, hxml):
        self.hxml = hxml

    @property
    def address(self):
        return self.hxml['yarn.resourcemanager.hostname'] + ':8088'

    def reload_queues():
        """
        Reloads queues. This calls other binaries using subprocess.
        """

        ret = subprocess.call('which yarn', shell=True)
        if ret != 0:
            print('You do not have the yarn binary on your system')
            print("Please manually run 'yarn rmadmin -refreshQueues'")
            return ret

        ret = subprocess.call('yarn rmadmin -refreshQueues', shell=True)
        if ret != 0:
            print("Refreshing queues failed, please manually run " +
                  "'yarn rmadmin -refreshQueues'")
            return ret

        return 0
