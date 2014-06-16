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

Full Configuration Spec
=======================
The configuration is a YAML file. There are three top-level items: fs,
scheduler, and sets. fs deals with the filesystem; this is typically HDFS.
Scheduler deals with the scheduler, most likely the Hadoop CapacityScheduler.
Finally, sets deals with hardware-dependent stuff. The hardware-dependent stuff
may affect the filesystem or the scheduler, and is done so that when you have a
heterogenous cluster, you don't have to worry about maintaining multiple sets
of configuration. In addition to supporting its own internal items and
sub-items, HAdmin supports a variety of Hadoop's own configuration items. These
Hadoop-specific items will also be listed below.

fs: Filesystem Configuration
----------------------------
fs supports the following sub-items:

* head: The address of the head filesystem node. For Hadoop 1, this should be a
  hostname:port combination, while Hadoop 2 only requires a hostname.

* bs: The block size of the filesystem, if the filesystem supports this as a
  configurable item.

* repmin: Minimum amount of replication allowed for the filesystem. For a Hadoop
  installation, this is typically 3.

* repmax: Maximum amount of replication allowed for the filesystem. For a Hadoop
  installation, I typically set this to 3 as well, although you can certainly
  set it higher.

* datadir: The directory that the head node stores its data in. For a Hadoop
  NameNode, this is the metadata of the whole filesystem and the folder should
  be mounted on a RAIDed set of drives.

* umask: The default umask for new files created.

* permissions: Whether or not permissions should be supported in the
  filesystem.  *Note: Due to YAML interpreting true and false as boolean
  values, you must quote them so that you are supplying either 'true' or
  'false' to avoid the boolean interpretation*

* http: The HTTP address that the head node can be reached at, usually to show
  a status page. For Hadoop, this should be a hostname:port value.

* safethresh: The safety threshold defines how many blocks should be accounted
  for before the filesystem goes from just being on to being write-accessible.
  For Hadoop, should be a floating point number from 0 to 1.

* safeext: The amount of time that the filesystem should delay its transition
  to write-accessibility after the safety threshold has been reached, in
  milliseconds.

* home: The home directory for users. Don't ask me why, but it's usually /user
  for Hadoop installations.

scheduler: Scheduler Configuration
----------------------------------
scheduler supports the following sub-items:

* head: Same as in the fs section.

* datadir: Same as in the fs section.

* maxjobs: Maximum number of jobs that can run.

* maxtpq: Assuming each job is split into tasks, the maximum number of tasks
  allowed to run on the entire cluster.

* maxtpu: Assuming each job is split into tasks, the maximum number of tasks
  each user is allowed to run.

* tmp: Directory that temporary data is stored on slave nodes.

* ulim: The maximum amount of queue capacity a single user is allowed use
  unless there are unused task slots available

* mapred.capacity-scheduler.default-init-accept-jobs-factor: Hadoop v1 key

* mapred.capacity-scheduler.default-user-limit-factor: Hadoop v1 key

* mapred.capacity-scheduler.default-supports-priority: Hadoop v1 key

* mapred.capacity-scheduler.init-poll-interval: Hadoop v1 key

* mapred.capacity-scheduler.init-worker-threads: Hadoop v1 key

* yarn.scheduler.capacity.maximum-am-resource-percent: Hadoop v2 key

* yarn.scheduler.capacity.node-locality-delay: Hadoop v2 key

* yarn.scheduler.capacity.resource-calculator: Hadoop v2 key

queues: Queue Configuration
---------------------------
queues is an interesting one. The sub-items of queues are the queue names; the
sub-items of queue names that are supported are:

* admins: CSV list of users that can administer the queue.

* mincap: The guaranteed minimum capacity (percent of total cluster capacity)
  that this queue gets to use.

* maxcap: The maximum capacity that this queue can use when some slots are
  empty

* maxtpu: Same as in scheduler, but for this queue.

* state: The state of the queue. For Hadoop, either RUNNING or STOPPED.

* ulim: Same as in scheduler, but for this queue.

* users: CSV list of users that can submit jobs to this queue.

sets: Set Configuration
-----------------------
sets is also an interesting one. The sub-items of sets are the set names; the
sub-items of set names that are supported are:

* datadir: CSV list of folders where slave nodes in this set will store
  permanent data. As in nodes that are running a filesystem (i.e. HDFS).

* cpus: The number of cores the machines in this set have.
