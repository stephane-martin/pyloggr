# encoding: utf-8

"""
GeoIP filter
"""

from __future__ import absolute_import, division, print_function

__author__ = 'stef'

import logging
from os.path import join, exists

import geoip2.database
from geoip2.errors import AddressNotFoundError
from future.builtins import str as text
from .base import Engine


RELATIVE_GEOLITE_FILENAME = 'GeoLite2-City.mmdb'

logger = logging.getLogger(__name__)


class GeoIPEngine(Engine):
    """
    A filter to geolocate IP addresses

    Pyloggr main configuration directory must contain a `geoip` subfolder. The `geoip` subfolder must
    contain the `GeoLite2-City.mmdb` geolocation database.

    This filter is (AFAIK) not thread-safe.

    Parameters
    ==========
    directory: str
        pyloggr main configuration directory
    """
    thread_safe = False

    def __init__(self, directory):
        super(GeoIPEngine, self).__init__(directory)
        geoip_conf_dir = join(directory, 'geoip')
        fname = join(geoip_conf_dir, RELATIVE_GEOLITE_FILENAME)
        self.fname = fname
        self.reader = None

    def open(self):
        """
        Initialize the GeoIP database
        """
        if self.reader is None:
            logger.info("Initialize GeoIP database")
            if not exists(self.fname):
                raise RuntimeError('GeoIP database doesnt exist at: {}'.format(self.fname))
            # MODE_MMAP: load the geoip database in memory
            self.reader = geoip2.database.Reader(self.fname, mode=geoip2.database.MODE_MMAP)

    def locate(self, ip_address, prefix=''):
        """
        Geolocate a IP.

        :param ip_address: IP address
        :type ip_address: str
        :param prefix: prefix for returned fields
        :type prefix: str
        :return:
        """
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
        """
        Free the GeoIP database
        """
        if self.reader is not None:
            logger.debug("Closing GeoIP database")
            self.reader.close()
            self.reader = None

    def apply(self, ev, args, kw):
        """
        Apply the GeoIP filter to an avent

        If it succeeds, some custom fields are added to the event:

        - continent
        - city
        - country
        - country_iso
        - subdivision
        - latitude
        - longitude

        The fields can be prefixed using the `prefix` kw parameter.

        :type ev: Event
        :type args: list or str
        :type kw: dict
        """
        prefix = kw.get('prefix', '')
        prefix = prefix[0] if prefix else ''
        success = False
        for ip in args:
            new_fields = self.locate(ip, prefix)
            if new_fields:
                success = True
                ev.update_cfields(new_fields)
        return success
