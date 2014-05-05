#!/usr/bin/env python

from setuptools import setup, find_packages
import hadmin.hadmin_test

setup(
        name = "HAdmin",
        version = "0.1",
        packages = find_packages(),
        author = "Alec Ten Harmsel",
        author_email = "alec@alectenharmsel.com",
        description = "A Hadoop configuration generator",
        url = "http://github.com/trozamon/hadmin",
        license = "MIT",

        test_suite = "hadmin.hadmin_test",

        setup_requires = ['flake8']
)
