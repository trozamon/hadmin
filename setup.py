#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='hadmin',
    version='0.2.dev',
    packages=find_packages(),
    author='Alec Ten Harmsel',
    author_email='alec@alectenharmsel.com',
    description='A Hadoop configuration manager',
    url='http://github.com/trozamon/hadmin',
    license='MIT',
    entry_points={
        'console_scripts': [
            'hadmin = hadmin.util:run'
            ]
        }
)
