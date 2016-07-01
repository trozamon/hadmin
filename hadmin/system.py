"""
"System" library
----------------

Get configuration and other "system" things. In general, these functions will
load configurations from ``/etc/hadoop/conf``.
"""


import hadmin.rest
from hadmin.util import HXML
from hadmin.yarn import CapacityScheduler, ResourceManager
import os


HADOOP_CONF_DIRS = [
        '.',
        '/etc/hadoop/conf'
        ]

CAPACITY_SCHEDULER_FILENAME = 'capacity-scheduler.xml'
YARN_FILENAME = 'yarn-site.xml'


def find_hxml_dir():
    for d in HADOOP_CONF_DIRS:
        if 'core-site.xml' in os.listdir(d):
            return d

    raise IOError("Can't find directory containing Hadoop configuration")


def find_hxml(filename):
    """
    Load an py:class:`HXML` from a file in a built-in hadoop configuration
    directory
    """

    d = find_hxml_dir()
    try:
        with open(os.path.join(d, filename), 'r') as f:
            return HXML.from_str(f.read())
    except IOError:
        pass

    return None


def get_cap():
    """
    Returns the system's :py:class:`hadmin.yarn.CapacityScheduler`
    """

    hxml = find_hxml(CAPACITY_SCHEDULER_FILENAME)
    return CapacityScheduler(hxml)


def save_cap(hxml):
    d = find_hxml_dir()
    hxml.save(os.path.join(d, CAPACITY_SCHEDULER_FILENAME))


def get_rm():
    """
    Returns the system's :py:class:`hadmin.yarn.ResourceManager`
    """

    hxml = find_hxml(YARN_FILENAME)
    return ResourceManager(hxml)


def rest_nm():
    """
    Returns a default :py:class:`hadmin.rest.NodeManager`
    """

    from hadmin.rest import NodeManager

    return NodeManager.load_from_host('localhost:8042',
                                      path=hadmin.rest.NM_INFO_PATH)


def rest_rm():
    """
    Returns a default :py:class:`hadmin.rest.ResourceManager`
    """

    rm = get_rm()

    paths = [hadmin.rest.RM_METRICS_PATH, hadmin.rest.RM_SCHEDULER_PATH]
    return hadmin.rest.ResourceManager.load_from_host(rm.address, paths=paths)
