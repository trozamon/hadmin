# hadmin

*This is my first python project. The python will no doubt be sloppy,
and I welcome improvements as will as criticism of my coding style.*

Hadmin is a Hadoop administration tool, mostly focused on reducing the
pain of adding users, queues, and adding nodes. There are a couple of
purposes:

* Provide command-line tools to update Hadoop config
* Provide a Python API to update Hadoop config

Current Hadoop versions supported are:

* 1.1.x

Current operations supported are:

* Adding and deleting users
* Adding, deleting, and modifying queues

## Read this first
The Hadmin project assumes a little familiarity with Hadoop, as well
as assumes that you're using the CapacityScheduler.

## Getting Started
Hadmin assumes that all work is carried out in the current directory. Hadmin
also stores all the config in YAML because it's a little easier to read
and not nearly as annoying as XML. So, you need to be in whatever directory
you want all this YAML to be stored in.

To generate the initial YAML configuration from existing Hadoop XML
configuration, run

    hadmin init <directory>

This will dump a bunch of YAML into the current directory that Hadmin will
modify and use to generate Hadoop XML.

## Generating Hadoop configuration
Generating Hadoop configuration can be accomplished by running

    hadoop generate <directory>

This command will generate Hadoop XML and throw it in the directory
specified on the command line.

## Queues
A queue can be added by running

    hadmin queueadd <queue> <user>

Adding a queue requires a user. This initial user will be added as both
a user and an administrator.

A queue can be deleted by running

    hadmin queuedel <queue>

A queue can be modified by running

    hadmin queuemod --capacity <cap> --maxcap <maxcap> --tpu <tpu> <queue>

At least one of --capacity, --maxcap, --tpu needs to be specified for
queuemod to actually do anything. --capacity changes the capacity of the
queue, --maxcap the maximum capacity of the queue, and --tpu the maximum
initialized tasks per user of the queue.

## Users
A user can be added to a queue by running

    hadmin useradd <user> <queue>

and an administrator can be added to a queue by running

    hadmin useradd --admin <user> <queue>

A user or administrator can be deleted by replacing 'useradd' in the above
commands with 'userdel'. Please note that --admin must be supplied to userdel
for admin deletion; 'hadmin userdel <user>' will not remove a user from the
admin list.

An administrator has the power to delete any job from the queue, while a user
has the power to add/delete his jobs to the queue.

## Future work
Setting the capacity of multiple queues is annoying to get correct right now
in Hadoop - it must add up to exactly 100. It would be convenient to be
able to specify two queues with capacities of 1 and have Hadmin figure out
that that's supposed to be 50 for both.

It'd also be nice to auto-generate sets of configs for machines with
different CPU/HDD setups as well.

A long-term goal of Hadmin is to intelligently generate as much config
dynamically as possible, hopefully leading to better configured clusters.
This could even include tuning performance automatically based on some
formulas to at least give sys admins a head start on performance tuning.

For any setting, leaving it blank will cause Hadmin to try and deduce a
smart value, taking into account number of users on the queue as well as
the hardware it's being run on.

## Helping out
On the off chance that you've stumbled on this project and want to
contribute, that's fantastic. You can help in any way you see fit, whether
that is refactoring, adding features, writing tests, writing documentation,
or testing it out on a cluster.
