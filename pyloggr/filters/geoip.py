# encoding: utf-8
__author__ = 'stef'

import logging
from os.path import join

import geoip2.database
from geoip2.errors import AddressNotFoundError
from future.builtins import str as text

from ..event import Event


RELATIVE_GEOLITE_FILENAME = 'GeoLite2-City.mmdb'

logger = logging.getLogger(__name__)


class GeoIPEngine(object):
    thread_safe = False

    def __init__(self, directory):
        geoip_conf_dir = join(directory, 'geoip')
        fname = join(geoip_conf_dir, RELATIVE_GEOLITE_FILENAME)
        self.fname = fname
        self.reader = None

    def open(self):
        if self.reader is None:
            logger.info("Initialize GeoIP database")
            # MODE_MMAP: load the geoip database in memory
            self.reader = geoip2.database.Reader(self.fname, mode=geoip2.database.MODE_MMAP)

    def locate(self, ip_address):
        if self.reader is None:
            logger.error("GeoIP database is not opened")
            return None
        try:
            city = self.reader.city(ip_address)
        except (AddressNotFoundError, ValueError):
            logger.info("GeoIP failed for: {}".format(ip_address))
            return None
        return {
            'continent': city.continent.name,
            'city': city.city.name,
            'country': city.country.name,
            'country_iso': city.country.iso_code,
            'subdivision': city.subdivisions.most_specific.name,
            'latitude': city.location.latitude,
            'longitude': city.location.longitude
        }

    def close(self):
        if self.reader is not None:
            logger.debug("Closing GeoIP database")
            self.reader.close()
            self.reader = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, ext_type, exc_value, traceback):
        self.close()

    def apply(self, ev, arguments):
        """
        :type ev: Event
        """
        target_ip = None
        if isinstance(arguments, text):
            target_ip = arguments
        elif isinstance(arguments, list):
            if len(arguments) > 0:
                target_ip = arguments[0]
            else:
                logger.error("GeoIP apply: empty list arguments")
        else:
            logger.error("GeoIP apply: unknown arguments type")
        if target_ip:
            new_fields = self.locate(target_ip)
            if new_fields:
                ev.update(self.locate(target_ip))
                return True
        return False
