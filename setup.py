from setuptools import setup, find_packages

setup(
    name='hadmin',
    version='0.1',
    packages=find_packages(),
    author='Alec Ten Harmsel',
    author_email='alec@alectenharmsel.com',
    description='A Hadoop configuration manager',
    url='http://github.com/trozamon/hadmin',
    license='MIT',
    test_suite='hadmin.test',
    tests_require=['flake8'],
    entry_points={
        'console_scripts': [
            'hadmin = hadmin.util:run'
            ]
        }
)
