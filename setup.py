from setuptools import setup, find_packages
import hadmin

desc = """HAdmin (Hadoop Admin) is a general hadoop administration tool. It
allows for creating users, queues, and groups. It also contains utilities for
exporiting metrics to graphite and influxdb""".replace('\n', ' ')

setup(
    name='hadmin',
    version=hadmin.__version__,
    packages=find_packages(),
    author='Alec Ten Harmsel',
    author_email='alec@alectenharmsel.com',
    description=desc,
    url='http://github.com/trozamon/hadmin',
    license='MIT',
    entry_points={
        'console_scripts': [
            'hadmin = hadmin.main:run'
            ]
        }
)
