hadmin
======

*This is my first python project. The python will no doubt be sloppy,
and I welcome improvements as will as criticism of my coding style.*

Hadoop (and related tools) administration tool. Manages config in
an easy way, and also generates XML on the fly for multi-tenant
environments. The purpose of Hadmin is two-fold:

* Provide command-line tools to update Hadoop config
* Provide a Python API to update Hadoop config

Hadmin (will) strives to support as many versions as possible. Right
now only 1.1.x is supported, but hopefully that'll change pretty quick.
Hopefully support can be easily added by way of some simple config, but
we'll see.

A long-term goal of Hadmin is to intelligently generate as much config
dynamically as possible, hopefully leading to better configured clusters.
This could even include tuning performance automatically based on some
formulas to at least give sys admins a head start on performance tuning.

Dynamically generating queues in a multi-tenanted environment is easy.
For a queue named staff, there are three files:

#### Future work
Sets of computers with different HDD configs
useradd and queueadd utilities

For any setting, leaving it blank will cause Hadmin to try and deduce a
smart value, taking into account number of users on the queue as well as
the hardware it's being run on.

## Helping out
On the off chance that you've stumbled on this project and want to
contribute, that's fantastic. It's pretty open-ended, but the structure
will be nailed down within a few months.
