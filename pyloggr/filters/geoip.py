# encoding: utf-8
__author__ = 'stef'

import logging
from os.path import join, exists

import geoip2.database
from geoip2.errors import AddressNotFoundError
from future.builtins import str as text


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
            if not exists(self.fname):
                raise RuntimeError('GeoIP database doesnt exist at: {}'.format(self.fname))
            self.reader = geoip2.database.Reader(self.fname, mode=geoip2.database.MODE_MMAP)

    def locate(self, ip_address, prefix=''):
        if self.reader is None:
            logger.error("GeoIP database is not opened")
            return None
        try:
            city = self.reader.city(ip_address)
        except (AddressNotFoundError, ValueError):
            logger.info("GeoIP failed for: {}".format(ip_address))
            return None
        return {
            prefix + 'continent': city.continent.name,
            prefix + 'city': city.city.name,
            prefix + 'country': city.country.name,
            prefix + 'country_iso': city.country.iso_code,
            prefix + 'subdivision': city.subdivisions.most_specific.name,
            prefix + 'latitude': city.location.latitude,
            prefix + 'longitude': city.location.longitude
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

    def apply(self, ev, args, kw):
        """
        :type ev: Event
        """
        target_ip = None
        if isinstance(args, text):
            target_ip = args
        elif isinstance(args, list):
            if len(args) > 0:
                target_ip = args[0]
            else:
                logger.error("GeoIP apply: empty list arguments")
        else:
            logger.error("GeoIP apply: unknown arguments type")
        if target_ip:
            prefix = kw.get('prefix', '')
            new_fields = self.locate(target_ip, prefix)
            if new_fields:
                ev.update_fields(new_fields)
                return True
        return False
