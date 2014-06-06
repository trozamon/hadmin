======
HAdmin
======

HAdmin is a Hadoop administration tool, mostly focused on reducing the
pain of adding users, queues, and adding nodes.

Current Hadoop versions supported are:

* 1.x
* 2.x

Current operations supported are:

* Adding and deleting users
* Adding, deleting, and modifying queues

Architecture
============
HAdmin maintains its own internal configuration to manage queues and
schedulers. It does this so that a number of backends can all use the same
simple frontend. HAdmin maintains its configuration in YAML because it's simple
to read and edit. The YAML is sorted as well, making it as VCS friendly as
possible.

Types
-----
As a part of validation, type checking is done on the configuration.  HAdmin
supports the following types of values in its configuration:

* num: A floating point or integer

* str: A string

* csv: A list of strings separated by commas

Please note that the YAML spec contains boolean (true/false) values; to get
around this, any value that is 'true' or 'false' needs to have quotes around
it. You should literally write out 'true' or 'false', with quotes.

Queue Model
-----------
HAdmin views a queue as having the following properties:

* Minimum Capacity: A queue has a certain guaranteed amount of the total
  compute capacity of the cluster it is running on

* Maximum Capacity: A queue has a limit to how much capacity of the cluster it
  can consume

* Running: A queue can be running or not running

* Administrators: A queue has administrators who can add and remove all jobs,
  no matter who the job owner is

* Users: A queue has users who can submit jobs and delete their own jobs

* User Limit: A queue has a limit on the percent of queue resources an
  individual user can submit

These are represented by configuration keys:

+----------------------+----------+------+
| Property             | Key      | Type |
+======================+==========+======+
| Administrators       | admins   | csv  |
+----------------------+----------+------+
| Minimum Capacity     | mincap   | num  |
+----------------------+----------+------+
| Maximum Capacity     | maxcap   | num  |
+----------------------+----------+------+
| Running              | running  | str  |
+----------------------+----------+------+
| User Limit           | ulim     | num  |
+----------------------+----------+------+
| Users                | users    | csv  |
+----------------------+----------+------+

Scheduler Model
---------------
To HAdmin, a scheduler has the following properties:

* Maximum Jobs: The maximum number of initialized jobs cluster-wide

* Maximum Tasks per Queue: Assuming jobs are split into tasks, the maximum
  amount of tasks in a single queue

* Maximum Tasks per User: Assuming jobs are split into tasks, the maximum
  amount of tasks that can be run by a single user

These are also represented by configuration keys:

+-------------------------+---------+------+
| Property                | Key     | Type |
+=========================+=========+======+
| Maximum Jobs            | maxjobs | num  |
+-------------------------+---------+------+
| Maximum Tasks per Queue | maxtpq  | num  |
+-------------------------+---------+------+
| Maximum Tasks per User  | maxtpu  | num  |
+-------------------------+---------+------+

Hardware Sets
-------------
Running Hadoop on a heterogenous cluster can be a little difficult. HAdmin
uses the concept of 'sets' of hardware that are used to generate multiple
sets of configurations for all your different hardware setups.

The main differences are usually the CPU and the disks.

scheduler:
  head: jt.blah.com:54311
  datadir: /var/hadoop/mapred/system
  tmp: /var/hadoop/mapred/local
fs:
  head: nn.blah.com:54310
  bs: 134217728
  repmin: 3
  repmax: 5
  datadir: /var/hadoop/name
  umask: 027
  permissions: 'true'
  http: nn.blah.com:50070
  safethresh: 0.999
  safeext: 5000
  home: /user
sets:
  oldies:
    datadir: /var/hadoop/data,/mnt/disk2/hadoop/data,/mnt/disk3/hadoop/data
    cpus: 16
  newbz:
    datadir: /var/hadoop/data/,/mnt/disk2/hadoop/data/,/mnt/disk3/hadoop/data/,/mnt/disk4/hadoop/data/,/mnt/disk5/hadoop/data/,/mnt/disk6/hadoop/data/,/mnt/disk7/hadoop/data/,/mnt/disk8/hadoop/data/,/mnt/disk9/hadoop/data/,/mnt/disk10/hadoop/data/,/mnt/disk11/hadoop/data/,/mnt/disk12/hadoop/data/
    cpus: 12

Using HAdmin
============

HAdmin config is YAML with two top-level sections: 'queues' and 'scheduler'.
The queue section contains a subsection for each queue, and each queue can
contain the keys listed in the Queue Model section. For reference, take a look
at the sample config provided with hadmin. You can get it into your current
directory by running `hadmin init`, or view it in the repo at
`data/hadmin.yaml`.

The scheduler section contains the keys listed in the scheduler model, and any
other config items used by your particular scheduler (i.e. Hadoop
CapacityScheduler).  Many schedulers have a bunch of different options and it's
impossible to abstract all of them away into a 'model', so I've only taken the
ones that appear to be universal. Full keys are also allowed in the scheduler
section, as you can see in the default configuration supplied by HAdmin.
