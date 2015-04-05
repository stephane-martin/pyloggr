# encoding: utf-8

"""
Monitor a directory on the filesystem, and parse new files as logs
"""

__author__ = 'stef'

SECONDS = 10

import logging
import os
from os.path import getsize, exists, isfile, join, abspath, basename

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from watchdog.events import FileCreatedEvent, FileModifiedEvent, FileMovedEvent, FileDeletedEvent
from tornado.gen import coroutine
from tornado.ioloop import IOLoop, PeriodicCallback

logger = logging.getLogger(__name__)


class HarvestHandler(FileSystemEventHandler):
    def __init__(self, process):
        FileSystemEventHandler.__init__(self)
        self.process = process

    # callbacks are called in a different thread, that's why we transfer to the main IOLoop
    def on_moved(self, event):
        if isinstance(event, FileMovedEvent):
            IOLoop.instance().add_callback(self.process.on_moved, event)

    def on_created(self, event):
        if isinstance(event, FileCreatedEvent):
            IOLoop.instance().add_callback(self.process.on_created, event)

    def on_modified(self, event):
        if isinstance(event, FileModifiedEvent):
            IOLoop.instance().add_callback(self.process.on_modified, event)

    def on_deleted(self, event):
        if isinstance(event, FileDeletedEvent):
            IOLoop.instance().add_callback(self.process.on_deleted, event)


class Harvest(object):
    def __init__(self, harvest_conf):
        """
        :type harvest_conf: pyloggr.config.HarvestConfig
        """
        self.event_handler = HarvestHandler(self)
        self.observer = Observer()
        self.observer.schedule(self.event_handler, harvest_conf.directory, recursive=False)
        self.observed_files = {}
        self.ready_files = []
        self.periodic = None
        self.conf = harvest_conf

    @coroutine
    def shutdown(self):
        self.observer.stop()
        self.observer.join()
        if self.periodic:
            self.periodic.stop()
            self.periodic = None

    @coroutine
    def launch(self):
        self.observed_files = {}
        for fname in os.listdir(self.conf.directory):
            abs_fname = abspath(join(self.conf.directory, fname))
            if isfile(abs_fname) and not basename(abs_fname).startswith('.'):
                self.observed_files[abs_fname] = {'size': getsize(abs_fname), 'seconds': 0}

        self.observer.start()
        self.periodic = PeriodicCallback(self.check, 1000)
        self.periodic.start()

    @coroutine
    def check(self):
        # check if the observed files have changed their size
        for fname in self.observed_files:
            fsize = getsize(fname)
            if self.observed_files[fname]['size'] == fsize:
                self.observed_files[fname]['seconds'] += 1
            else:
                self.observed_files[fname] = {'size': fsize, 'seconds': 0}

        stalled_files = {fname for fname in self.observed_files if self.observed_files[fname]['seconds'] >= SECONDS}
        for fname in stalled_files:
            del self.observed_files[fname]
        stalled_files = {fname for fname in stalled_files if exists(fname)}
        zero_size_files = {fname for fname in stalled_files if getsize(fname) == 0}
        stalled_files.difference_update(zero_size_files)
        for fname in zero_size_files:
            os.remove(fname)

        for fname in stalled_files:
            yield self.upload(fname)
            if self.conf.remove_after:
                os.remove(fname)

    @coroutine
    def upload(self, fname):
        # try to parse fname for logs
        print "***", fname


    @coroutine
    def on_moved(self, event):
        """
        :param event: 'file moved' event
        :type event: FileMovedEvent
        """
        if not basename(event.dest_path).startswith('.'):
            self.observed_files[abspath(event.dest_path)] = {'size': getsize(event.dest_path), 'seconds': 0}
        if abspath(event.src_path) in self.observed_files:
            del self.observed_files[abspath(event.src_path)]

    @coroutine
    def on_created(self, event):
        """
        :param event: 'file created' event
        :type event: FileCreatedEvent
        """
        if not basename(event.src_path).startswith('.'):
            self.observed_files[abspath(event.src_path)] = {'size': 0, 'seconds': 0}

    @coroutine
    def on_modified(self, event):
        """
        :param event: 'file modified' event
        :type event: FileModifiedEvent
        """
        if basename(event.src_path).startswith('.'):
            return
        try:
            self.observed_files[abspath(event.src_path)] = {'size': getsize(event.src_path), 'seconds': 0}
        except OSError as ex:
            if ex.errno == 2:
                # file has been deleted...
                pass
            else:
                raise

    @coroutine
    def on_deleted(self, event):
        """
        :param event: 'file deleted' event
        :type event: FileDeletedEvent
        """
        if abspath(event.src_path) in self.observed_files:
            del self.observed_files[abspath(event.src_path)]


