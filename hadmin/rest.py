"""
Hadoop REST API handling
------------------------

"""

import json

try:
    from http.client import HTTPConnection
except ImportError:
    from httplib import HTTPConnection


# NodeManager paths
NM_INFO_PATH = '/ws/v1/node'

# ResourceManager paths
RM_METRICS_PATH = '/ws/v1/cluster/metrics'
RM_SCHEDULER_PATH = '/ws/v1/cluster/scheduler'


class Base:
    """
    Handles common networking, JSON, etc. for REST interfaces.

    Subclasses must implement a function with the signature '__init__(dict)'
    and 'load(dict)'.
    """

    def __init__(self):
        raise AttributeError("You cannot initialize this class")

    @classmethod
    def load_from_host(cls, addr, path=None, paths=[]):
        if path:
            paths.append(path)

        conn = HTTPConnection(addr)
        return cls.load_from_connections(conn, paths)

    @classmethod
    def load_from_connection(cls, conn, path):
        conn.request('GET', path)
        res = conn.getresponse()
        if res.status == 200:
            return cls.load_from_json(res.read())

    @classmethod
    def load_from_connections(cls, conn, paths):
        jsons = []
        for path in paths:
            conn.request('GET', path)
            res = conn.getresponse()
            if res.status == 200:
                jsons.append(res.read())

    @classmethod
    def load_from_json(cls, raw_json):
        tmp = dict()

        try:
            tmp = json.loads(raw_json)
        except ValueError:
            pass
        except KeyError:
            pass

        return cls(tmp)

    @classmethod
    def load_from_jsons(cls, raw_jsons):
        obj = None

        for raw_json in raw_jsons:
            tmp = dict()

            try:
                tmp = json.loads(raw_json)
            except ValueError:
                pass
            except KeyError:
                pass

            if obj:
                obj.load(tmp)
            else:
                obj = cls(tmp)

        return obj


class NodeManager(Base):
    """
    Wrapper around the NodeManager REST interface

    Currently supports the node info only, not applications or containers.
    """

    def __init__(self, obj=dict()):
        self.load(obj)

    def load(self, data=dict()):
        if data:
            self.data = data['nodeInfo']
        else:
            self.data = dict()

    def isHealthy(self):
        return self.data['nodeHealthy']

    def getHealthReport(self):
        return self.data['healthReport']

    @property
    def allocated_memory(self):
        return self.data['totalPmemAllocatedContainersMB']

    @property
    def allocated_cores(self):
        return self.data['totalVCoresAllocatedContainers']


class Queue:
    """
    REST representation of a queue

    Has these properties:

    * absolute_capacity_used - amount of cluster's max capacity it is using
    * applications - number of applications currently running
    * capacity_used - amount of this queue's max capacity it is using
    * containers - Number of containers used. Only leaf queues can have
                   non-zero values
    * name - fully qualified name of the queue
    * resources_used_vcpus - Number of virtual CPUs in use
    * resources_used_memory_mb - Amount of memory (in MB) in use
    """

    def __init__(self, queue_dict, parent_name='root'):
        self.absolute_capacity_used = queue_dict['absoluteUsedCapacity']
        self.applications = queue_dict['numApplications']
        self.capacity_used = queue_dict['usedCapacity']
        self.name = '.'.join([parent_name, queue_dict['queueName']])
        self.resources_used_memory_mb = queue_dict['resourcesUsed']['memory']
        self.resources_used_vcpus = queue_dict['resourcesUsed']['vCores']

        if 'numContainers' in queue_dict:
            self.containers = queue_dict['numContainers']
        else:
            self.containers = 0


class ResourceManager(Base):
    """
    Wrapper around the ResourceManager REST interface.

    Currently supports the metrics and scheduler APIs.

    Properties:

    * apps_completed - Number of completed applications
    * apps_failed - Number of applications that ended in failure
    * apps_killed - Number of applications that were killed by a user
    * apps_pending - Number of waiting-to-run applications
    * apps_running - Number of running applications
    * apps_submitted - Number of submitted applications
    * capacity_max - Maximum cluster capacity
    * capacity_used - Currently used cluster capacity
    * memory_mb_allocated - Amount of memory (in MB) allocated for jobs
    * memory_mb_reserved - Amount of memory (in MB) reserved for future jobs
    * memory_mb_total - Amount of total cluster memory (in MB)
    * nodes_active - Number of active (online) nodes
    * nodes_decommissioned - Number of decommissioned nodes
    * nodes_total - Number of total (active + decommissioned + unhealthy) nodes
    * nodes_unhealthy - Number of unhealthy nodes
    * queues - Array of Queue objects
    * vcpus_allocated - Number of vCPUs allocated for jobs
    * vcpus_reserved - Number of vCPUs reserved for future jobs
    * vcpus_total - Number of total cluster vCPUs
    """

    def __init__(self, obj=dict()):
        self.load(obj)

    def load(self, obj=dict()):
        if 'clusterMetrics' in obj:
            self.load_metrics(obj)
        elif 'scheduler' in obj:
            self.load_scheduler(obj)

    def load_scheduler(self, data=dict()):
        # Scheduling stuff
        self._scheduler = data['scheduler']['schedulerInfo']
        self.capacity_used = self._scheduler['usedCapacity']
        self.capacity_max = self._scheduler['maxCapacity']

        self.queues = []
        self._load_subqueues(self._scheduler['queues']['queue'])

    def _load_subqueues(self, queue_list, prefix='root'):
        for q in queue_list:
            tmp = Queue(q, prefix)
            self.queues.append(tmp)

            if 'queues' in q:
                self._load_subqueues(q['queues']['queue'],
                                     prefix=tmp.name)

    def load_metrics(self, data=dict()):
        met = data['clusterMetrics']

        self.apps_completed = met['appsCompleted']
        self.apps_failed = met['appsFailed']
        self.apps_killed = met['appsKilled']
        self.apps_pending = met['appsPending']
        self.apps_running = met['appsRunning']
        self.apps_submitted = met['appsSubmitted']

        self.memory_mb_allocated = met['allocatedMB']
        self.memory_mb_reserved = met['reservedMB']
        self.memory_mb_total = met['totalMB']

        self.nodes_active = met['activeNodes']
        self.nodes_decommissioned = met['decommissionedNodes']
        self.nodes_total = met['totalNodes']
        self.nodes_unhealthy = met['unhealthyNodes']

        self.vcpus_allocated = met['allocatedVirtualCores']
        self.vcpus_reserved = met['reservedVirtualCores']
        self.vcpus_total = met['totalVirtualCores']
