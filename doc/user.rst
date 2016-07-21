User Guide
==========

HAdmin exists to make the process of common Hadoop administration operations
faster and/or easier. It supports operations like checking the health of the
various daemons, generating ``CapacityScheduler`` queue configuration from YAML
files, modifying ``CapacityScheduler`` queue configuration in place for
temporary reasons, and checking that your HDFS layout is reasonably standard.

Commands
--------
All of HAdmin's commands are invoked as subcommands of the ``hadmin``
executable.  For example, to run the HDFS standards checker, run ``hadmin
fhs``.

chk-dn
++++++
Check the health of the DataNode. Usage::

    # Check the DataNode running on this box
    hadmin chk-dn

    # Check the DataNode running on dn01.example.com
    hadmin chk-dn dn01.example.com

chk-nm
++++++
Check the health of the NodeManager. Usage::

    # Check the NodeManager on this box
    hadmin chk-nm

    # Check the NodeManager running on dn01.example.com
    hadmin chk-nm dn01.example.com

fhs
+++
Check and optionally fix up the standard directories and permissions in HDFS.
Just like a Linux filesystem, HDFS needs to have a few directories in place in
order for the rest of the Hadoop ecosystem to work properly:

* /
* /tmp
* /user

In addition, each user needs a directory. For user ``bob``, the directory
``/user/bob`` needs to exist with the correct permissions. By utilizing this
command, an admin can check for problems in an automated fashion::

    # Check for problems
    hadmin fhs

    # Fix problems that exist
    hadmin fhs --fixup

genqueues
+++++++++
Generate queues from a bunch of HAdmin-specific YAML files. The YAML files can
be in any directory. The name of the file will be the name of the queue;
``default.yml`` will be generated into a queue named ``default``. The schema of
the YAML files is::

    ---

    # List of queue administrators
    # Takes an array of strings
    admins:
      - list
      - of
      - admins

    # List of queue users
    # Takes an array of strings
    users:
      - list
      - of
      - users

    # Whether or not the queue is accepting job submission
    # Takes a boolean
    running: true

    # Capacity configuration. Both take integers or floating points
    capacity:

      # Maximum capacity. Must be in the range [0, 100]
      max: 100

      # Queue weight (explained below)
      weight: 1

    # Maximum amount of a queue's capacity that a single user is
    # allowed to use
    # 
    # i.e. Setting at 50 means a single user will only be able to
    # utilize 50% of the queue's guaranteed/minimum capacity.
    #
    # Takes a floating point or an integer
    user_limit: 50.0

The capacity weight is used to eliminate the need for babysitting queue
configurations when doing a lot of queue addition. All the queues at every
single level in the hierarchy require the sum of their capacities to be 100.
Instead of manually setting the capacity, setting the weight allows HAdmin to
enforce the "sum to 100" constraint automatically.

For example, having two HAdmin queues with a weight of 1 will generate two
Hadoop queues with a capacity of 50. Adding a third HAdmin queue with a weight
of 1 will generate three Hadoop queues, each with a capacity of 33.3.

Changing the weight of one of the HAdmin queues to 2 will change the resulting
Hadoop queues to have capacities of 25, 25, and 50.

queuecap
++++++++
**Deprecated**. Utilize YAML and ``hadmin genqueues`` instead.

Change the capacity or maximum capacity of a queue. Directly edits
``capacity-scheduler.xml``. Usage::

    # Change the minimum capacity of queue 'default' to 20
    hadmin queuecap default 20

    # Change the maximum capacity of queue 'default' to 80
    hadmin queuecap --max default 80

queueoff
++++++++
**Deprecated**. Utilize YAML and ``hadmin genqueues`` instead.

Turn a queue off; disable job submission to a particular queue and its child
queues. Usage::

    # Turn off the queue 'default'
    hadmin queueoff default

queueon
+++++++
**Deprecated**. Utilize YAML and ``hadmin genqueues`` instead.

Turn a queue on; enable job submission to a particular queue and its child
queues (that are also on). Usage::

    # Turn on the queue 'default'
    hadmin queueon default

queuestat
+++++++++
Print out a bunch of queue statistics for a queue and its subqueues. Usage::

    # Print out all the queue stats
    hadmin queuestat

    # Print out only the default queue stats
    hadmin queuestat default

queueulim
+++++++++
**Deprecated**. Utilize YAML and ``hadmin genqueues`` instead.

Change a queue's user limit factor. Usage::

    # Change the user limit factor for queue 'default' to 20
    hadmin queueulim default 20

sc
++
Perform a sanity check on the Hadoop CapacityScheduler configuration. Usage::

    # Run the sanity checker
    hadmin sc

stats-nm
++++++++
Print out some NodeManager statistics. Usage::

    # Print out the stats
    hadmin stats-nm

stats-rm
++++++++
Print out some ResourceManager statistics. Usage::

    # Print out the stats
    hadmin stats-rm

useradd
+++++++
**Deprecated**. Utilize YAML, ``hadmin genqueues``, and ``hadmin fhs`` instead.

Add a user or admin to a queue and create a directory in HDFS, if needed.
Usage::

    # Add user 'alec' to queue 'dev'
    hadmin useradd alec dev

    # Add admin 'alec' to queue 'dev'
    hadmin useradd --admin alec dev

userdel
+++++++
**Deprecated**. Utilize YAML and ``hadmin genqueues`` instead.

Remove a user or admin from a queue. Usage::

    # Remove user 'alec' from queue 'dev'
    hadmin userdel alec dev

    # Remove admin 'alec' from queue 'dev'
    hadmin userdel --admin alec dev
