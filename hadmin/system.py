"""
"System" library
----------------

Get configuration and other "system" things. In general, these functions will
load configurations from ``/etc/hadoop/conf``.
"""


from hadmin.util import CAPACITY_SCHEDULER_FILENAME, find_hxml
from hadmin.yarn import CapacityScheduler


def get_system_capacity_scheduler():
    """
    Returns the system's :py:class:`hadmin.yarn.CapacityScheduler`
    """

    hxml = find_hxml(CAPACITY_SCHEDULER_FILENAME)
    return CapacityScheduler(hxml)
