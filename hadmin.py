#!/usr/bin/env python

import sys
import os
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import argparse
import hadmin.cmd

def print_help():
    """ Prints out the help message """
    tmp = "Usage: hadmin <command> <command options>\n\n"
    tmp = tmp + "Commands:\n"
    tmp = tmp + "\t- useradd\n"
    tmp = tmp + "\t- queueadd\n"
    tmp = tmp + "\t- userdel\n"
    tmp = tmp + "\t- queuedel\n"
    print(tmp)

def run():
    if (len(sys.argv) <= 1 or sys.argv[1] == "-h"):
        print_help()
        return 0
    command = sys.argv[1]
    sysargs = sys.argv[2:]

    return getattr(hadmin.cmd, command)(sysargs)

if __name__ == "__main__":
    sys.exit(run())
