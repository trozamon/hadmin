# HAdmin

*Note: This is my first python project. The python will no doubt be sloppy,
and I welcome improvements as will as criticism of my coding style.*

HAdmin is a Hadoop administration tool, mostly focused on reducing the
pain of adding users and queues.

Currently, only Hadoop 2.x using the CapacityScheduler is supported. Hadmin
does:

* Adding and deleting users
* Adding, deleting, and modifying queues
* Sanity checks

## Using Hadmin
To modify users and queues, you must be in the directory that contains
`capacity-scheduler.xml`. Hadmin modifies the CapacityScheduler configuration
in place and automatically forces a queue reload and creates directories as
needed.

## User Management
### Adding Users
`hadmin useradd [--admin] <user> <queue>` adds users (admins with `--admin`) to
the specified queues.  Additionally, it will try to run the following commands:

* `hdfs dfs -ls /user/$user || hdfs dfs -mkdir /user/$user`
* `hdfs dfs -chown $user /user/$user`
* `yarn rmadmin -refreshQueues`

For example, if you want to add two new users, dan and bob, to an already
existing queue `root.marketing.twitter` to do marketing research based on
tweets, just run:

    hadmin useradd dan,bob marketing.twitter

Way easier than editing XML.

### Removing Users
`hadmin userdel [--admin] <user> <queue>` removes users (admins with `--admin`)
from the specified queues.  Additionally, it will try to run the following
command:

* `yarn rmadmin -refreshQueues`

## Queue Management
### Queue Statistics
`hadmin queuestat` will print in human-readable format a summary of your
current queues and various data about them.

### Adding Queues
`hadmin queueadd <queue> <user>` adds a queue named `queue` with `user` as an
adminstrator **and** a user. It runs the following commands as well:

* `hdfs dfs -ls /user/$user || hdfs dfs -mkdir /user/$user`
* `hdfs dfs -chown $user /user/$user`
* `yarn rmadmin -refreshQueues`

### Removing Queues
`hadmin queuedel <queue>` is aliased to `hadmin queueoff <queue>`. Since queues
can only be removed by restarting the ResourceManager, you must add `-f` to
force `hadmin` to remove the queue. This is almost never desirable behavior,
hence the `-f`.

### Modifying Queues
* `hadmin queueon <queue>` turns a queue on
* `hadmin queueoff <queue>` turns a queue off
* `hadmin queueulim <queue> <ulim>` sets the user limit factor of a queue
* `hadmin queuecap [--max] <queue> <cap>` sets the capacity of a queue, or with
  `--max` sets the maximum capacity.

### Sanity Checking
Running `hadmin sc` will run a sanity check of capacity-scheduler.xml and
report any potential flaws.

Additionally, a sanity check is automatically run before making any calls to
the `yarn` binary to ensure a stable scheduler.

## Helping out
On the off chance that you've stumbled on this project and want to contribute,
that's fantastic. You can help in any way you see fit, whether that is
refactoring, adding features, writing tests, writing documentation, or testing
it out on a cluster.
