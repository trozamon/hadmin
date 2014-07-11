# HAdmin

*Note: This is my first python project. The python will no doubt be sloppy,
and I welcome improvements as will as criticism of my coding style.*

HAdmin is a Hadoop administration tool, mostly focused on reducing the
pain of adding users, queues, and adding nodes.

Currently, only Hadoop 2.x is supported.

Current operations supported are:

* Adding and deleting users
* Adding and deleting queues

## Read this first
The HAdmin project assumes a little familiarity with Hadoop, as well
as assumes that you're using the CapacityScheduler.

To modify users and queues, you must be in the directory that contains
capacity-scheduler.xml.

## Users

### Adding Users
`hadmin useradd [-a|--admin] <user> <queue>` adds users, or admins with `-a` or
`--admin`, to the specified queues.  Additionally, it will try to run the
following commands:

* `hdfs dfs -ls /user/$user || hdfs dfs -mkdir /user/$user`
* `hdfs dfs -chown $user /user/$user`
* `yarn rmadmin -refreshQueues`

### Removing Users
`hadmin userdel [-a|--admin] <user> <queue>` removes users, or admins with `-a`
or `--admin`, from the specified queues.  Additionally, it will try to run the
following commands:

* `hdfs dfs -rm -r /user/$user`
* `yarn rmadmin -refreshQueues`

## Queues

### Adding Queues
`hadmin queueadd <queue> <user>` adds a queue with `$user` as an adminstrator
and a user. It runs the following commands as well:

* `hdfs dfs -ls /user/$user || hdfs dfs -mkdir /user/$user`
* `hdfs dfs -chown $user /user/$user`
* `yarn rmadmin -refreshQueues`

By default, queues are turned off and have 0 for both capacity and maximum
capacity.

### Removing Queues
`hadmin queuedel <queue>` is aliased to `hadmin queueoff <queue>`. Queues
can only be removed by restarting the ResourceManager. Adding `-f` will force
`hadmin` to remove the queue, requiring a ResourceManager restart. This is
almost never desirable behavior, hence the `-f`.

### Modifying Queues
**Note: Not yet supported**

* `hadmin queueon <queue>` turns a queue on
* `hadmin queueoff <queue>` turns a queue off
* `hadmin queuecap [-m|--max] <queue> <cap>` sets the capacity of a queue, or with
  `-m` or `--max` sets the maximum capacity.

## Helping out
On the off chance that you've stumbled on this project and want to
contribute, that's fantastic. You can help in any way you see fit, whether
that is refactoring, adding features, writing tests, writing documentation,
or testing it out on a cluster.
