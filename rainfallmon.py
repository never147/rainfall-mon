#!/usr/bin/env python
"""Monitor rainfall using the Environment Agency's Rainfall API

See https://environment.data.gov.uk/flood-monitoring/doc/rainfall
"""

from requests import Session
from datetime import datetime

import logging
import argparse
import socket


READINGS_API = '{root}/id/stations/{id}/readings'
API_ROOT = 'http://environment.data.gov.uk/flood-monitoring'
STATION_ID = '52201'

CARBON_HOST = 'localhost'
CARBON_PORT = 2003

LOGGER = logging.getLogger('rainfallmon')


def graphite_send(metric, host=CARBON_HOST, port=CARBON_PORT):
    message = ' '.join(metric).encode('ascii')
    LOGGER.debug("Sending message: {} (to {}:{})"
                 .format(message, host, port))
    so = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    so.connect((host, port))
    so.sendall(message + '\n')
    so.close()


class Station(object):

    def __init__(self, station=STATION_ID, url=API_ROOT, api=READINGS_API):
        self.session = Session()
        self.station = station
        self.url = url
        self.api = '{root}/id/stations/{id}'.format(root=url, id=station)
        # self.params = {'latest': True}

    def set_attrs(self):
        attrs = self.session.get(self.api).json()
        for key, value in attrs.items():
            setattr(self, key, value)

    def get_readings(self, params=None):
        if params is None:
            params = {'latest': True}
        readings = self.session.get(self.api + '/readings',
                                    params=params)
        return readings.json()['items']

    def metrics(self):
        res = []
        for reading in self.get_readings():
            datestamp = datetime.strptime(reading['dateTime'],
                                          '%Y-%m-%dT%H:%M:%SZ')
            timestamp = datetime.strftime(datestamp, "%s")
            metric = ('environment.rainfall.station_{}'.format(self.station),
                      str(reading['value']),
                      timestamp)
            res.append(metric)
        return res


def parse_args():
    parser = argparse.ArgumentParser('Get weather data and send to graphite')
    parser.add_argument('--log-level', '-l',
                        dest='log_level',
                        default=None,
                        choices=[None, 'info', 'debug'])
    parser.add_argument('--station', '-s',
                        default=[STATION_ID],
                        action='append',
                        help="Weather station ID. Default: %(default)s")
    parser.add_argument('--carbon-host', '-c',
                        dest='host',
                        help="Host IP for plain text carbon server."
                             " Default: %(default)s",
                        default=CARBON_HOST)
    parser.add_argument('--carbon-port', '-p',
                        dest='port',
                        help="Host port for plain text carbon server."
                             " Default: %(default)s",
                        default=CARBON_PORT,
                        type=int)
    return parser.parse_args()


def setup_logging(level=None):
    if level is not None:
        levels = {'info': logging.INFO, 'debug': logging.DEBUG}
        logging.basicConfig(level=levels[level])


def main():
    args = parse_args()
    setup_logging(args.log_level)
    metrics = []
    for station_id in set(args.station):
        station = Station(station=station_id)
        metrics.extend(station.metrics())
    for metric in metrics:
        graphite_send(metric=metric, host=args.host, port=args.port)


if __name__ == '__main__':
    main()
