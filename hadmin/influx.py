"""
InfluxDB-related functionality
------------------------------

Include `hadmin-stats-influxd`.
"""

import argparse
import hadmin.system
import requests
from requests.auth import HTTPBasicAuth
import sys
import time


SEC_TO_NANOSEC = 10**9


class WriteBody:

    def __init__(self):
        self.body = []

    def sanitize_name(self, name):
        bad_chars = ['-', '.']
        sanitized = name

        for c in bad_chars:
            sanitized = sanitized.replace(c, '_')

        while '__' in sanitized:
            sanitized = sanitized.replace('__', '_')

        return sanitized

    def add_measurement(self, name, value, timestamp, tag_string=None):
        """
        Add a measurement.

        timestamp should be a timestamp in seconds. may be floating point or
        integer.
        """

        tmp = self.sanitize_name(name)
        if tag_string:
            tmp += ','
            tmp += tag_string

        tmp += ' '
        tmp += 'value=' + str(value)
        tmp += ' '

        tmp += str(int(timestamp * SEC_TO_NANOSEC))

        self.body.append(tmp)

    def __str__(self):
        return "\n".join(self.body)


class Relay:
    """
    Sends statistics to InfluxDB.

    Takes an argument array (i.e. sys.argv) and configures itself from that.
    """

    COMPONENTS = {
                  'NodeManager': hadmin.system.rest_nm,
                  'ResourceManager': hadmin.system.rest_rm
                 }

    def __init__(self, args):
        self._username = None
        self._password = None
        self._tag_string = None
        self._interval = 10
        self.args = self.parse_args(args)

    def run(self):
        print('Sending metrics from ' + self.args.component + ' to ' +
              self.args.influxdb_address)
        print('Using database ' + self.args.database)

        if self.using_auth():
            print('Using username ' + self.username)

        if self.tag_string:
            print('Adding tag string "' + self.tag_string +
                  '" to requests')

        while True:
            body = self.get_request()
            auth = self.get_auth()

            resp = requests.post(self.args.influxdb_address + '/write',
                                 auth=auth,
                                 params={'db': self.args.database},
                                 data=str(body))

            if resp.status_code != 204:
                if resp.status_code == 200:
                    print('InfluxDB could not process the request')
                elif resp.status_code == 404:
                    print('Database ' + self.args.database + ' does not exist')
                else:
                    print('Failed to write request (' + str(resp.status_code) +
                          '):')
                    print(str(body))

            time.sleep(self.interval)

        return 0

    def get_request(self):
        thing = Relay.COMPONENTS[self.args.component]()
        req = WriteBody()
        t = time.time()

        d = dict(thing)
        for key in d:
            req.add_measurement(key, d[key], t, self.tag_string)

        return req

    def get_auth(self):
        if self.using_auth():
            return HTTPBasicAuth(self.username, self.password)

        return None

    def using_auth(self):
        if self.username and self.password:
            return True

        return False

    def parse_args(self, args):
        parser = argparse.ArgumentParser(prog='hadmin-stats-influxd',
                                         description='send metrics to influx')

        # Optional args
        parser.add_argument('--tag-string', nargs=1, dest='tag_string')
        parser.add_argument('--interval', nargs=1)
        parser.add_argument('--username', nargs=1)
        parser.add_argument('--password', nargs=1)

        parser.add_argument('influxdb_address')
        parser.add_argument('database')
        parser.add_argument('component',
                            help="one of " +
                            ', '.join(sorted(Relay.COMPONENTS.keys())))

        return parser.parse_args(args)

    @property
    def username(self):
        if self._username:
            return self._username

        if self.args.username:
            self._username = self.args.username[0]

        return self._username

    @property
    def password(self):
        if self._password:
            return self._password

        if self.args.password:
            self._password = self.args.password[0]

        return self._password

    @property
    def interval(self):
        if self._interval:
            return self._interval

        if self.args.interval:
            self._interval = int(self.args.interval[0])

        return self._tag_string

    @property
    def tag_string(self):
        if self._tag_string:
            return self._tag_string

        if self.args.tag_string:
            self._tag_string = self.args.tag_string[0]

        return self._tag_string


def run():
    r = Relay(sys.argv[1:])
    return r.run()
