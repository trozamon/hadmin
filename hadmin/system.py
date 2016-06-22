"""
"System" library
----------------

Get configuration and other "system" things. In general, these functions will
load configurations from ``/etc/hadoop/conf``.
"""


from hadmin.util import HXML
from hadmin.yarn import CapacityScheduler
import os


HADOOP_CONF_DIRS = [
        '.',
        '/etc/hadoop/conf'
        ]

CAPACITY_SCHEDULER_FILENAME = 'capacity-scheduler.xml'


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
