# encoding: utf-8

"""
Monitor a directory on the filesystem, and parse new files as logs
"""

__author__ = 'stef'

SECONDS = 10

import logging
import os
from os.path import getsize, exists, isfile, join, abspath, basename
from io import open

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from watchdog.events import FileCreatedEvent, FileModifiedEvent, FileMovedEvent, FileDeletedEvent
from tornado.gen import coroutine, Return, sleep
from tornado.ioloop import IOLoop, PeriodicCallback

from pyloggr.rabbitmq.publisher import Publisher, RabbitMQConnectionError
from pyloggr.event import Event
from pyloggr.packers import BasePacker
from pyloggr.utils import sleep

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
    def __init__(self, harvest_config, to_rabbitmq_config):
        """
        :type harvest_config: pyloggr.config.HarvestConfig
        """
        self.observers = dict()
        self.observed_files = {}
        self.ready_files = []
        self.periodic = None
        self.conf = harvest_config
        self.rabbitmq_config = to_rabbitmq_config
        self.observed_directories = []
        self._publisher = None
        self._closed_ev = None
        self.uploading_list = []

        self.observe_directories()

    def observe_directories(self):
        # check that directories exist and are writeable
        for directory_name in self.conf.directories:
            if not exists(directory_name):
                # exception will be eventually handled by processes.py, if directory_name can't be created
                os.makedirs(directory_name)
            if not os.access(directory_name, os.W_OK | os.X_OK):
                raise OSError("'{}' exists but is not writeable".format(directory_name))

        # set up observers
        for directory_name in self.conf.directories:
            self.observers[directory_name] = Observer()
            self.observers[directory_name].schedule(
                HarvestHandler(self), directory_name, recursive=self.conf.directories[directory_name].recursive
            )

    @coroutine
    def shutdown(self):
        for observer in self.observers.values():
            observer.stop()
            observer.join()
        if self.periodic:
            self.periodic.stop()
            self.periodic = None
        if self._publisher:
            yield self._publisher.stop()
            self._publisher = None

    @coroutine
    def launch(self):
        self.observed_files = {}
        for directory_name in self.conf.directories:
            for fname in os.listdir(directory_name):
                abs_fname = abspath(join(directory_name, fname))
                if isfile(abs_fname) and not basename(abs_fname).startswith('.'):
                    self.observed_files[abs_fname] = {'size': getsize(abs_fname), 'seconds': 0}

        for observer in self.observers.values():
            observer.start()

        self.periodic = PeriodicCallback(self.check, 1000)
        self.periodic.start()

    @coroutine
    def check(self):
        # check if the observed files sizes have changed
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
            match_dir_name = {
                directory_name for directory_name in self.conf.directories if fname.startswith(directory_name)
            }.pop()
            yield self.upload(fname, self.conf.directories[match_dir_name])

    @coroutine
    def upload(self, fname, directory_obj):
        """
        Parse file `fname` and publish lines as logs to Rabbitmq
        :type fname: str
        :type directory_obj: pyloggr.config.HarvestDirectory
        """
        # dont upload twice...
        if fname in self.uploading_list:
            return
        self.uploading_list.append(fname)

        try:
            current_publisher, closed_ev = yield self.get_publisher()
        except RabbitMQConnectionError:
            # no connection to rabbitmq
            yield sleep(60)
            self.uploading_list.remove(fname)
            return

        for packer_partial in reversed(directory_obj.packer_group.packers):
            current_publisher = packer_partial(current_publisher)

        results = []

        nb_publications = 0

        def after_published(future):
            res, _ = future.result()
            results.append(res)

        with open(fname, mode='rb') as fhandle:
            while not closed_ev.is_set():
                line = fhandle.readline()
                if len(line) == 0:
                    # EOF
                    break
                line = line.strip('\r\n ')
                if len(line) == 0:
                    continue
                event = Event(
                    severity=directory_obj.severity,
                    facility=directory_obj.facility,
                    app_name=directory_obj.app_name,
                    source=directory_obj.source,
                    message=line
                )
                event.add_tags("harvested")
                event["directory"] = directory_obj.directory_name
                event["fname"] = fname
                nb_publications += 1
                IOLoop.instance().add_future(current_publisher.publish_event(event), after_published)
                yield []

        # wait that all lines have been published
        while len(results) != nb_publications:
            yield sleep(1)
        # fname is finished
        if isinstance(current_publisher, BasePacker):
            # stop the packer
            current_publisher.shutdown()
        if closed_ev.is_set():
            # we lost rabbitmq connection somewhen...
            self._publisher = None
            yield sleep(60)
            self.uploading_list.remove(fname)
            return
        if not all(results):
            # some publications have failed
            yield sleep(60)
            self.uploading_list.remove(fname)
            return
        # everything good, remove the source file
        os.remove(fname)

    @coroutine
    def get_publisher(self):
        if self._publisher is not None:
            raise Return((self._publisher, self._closed_ev))
        self._publisher = Publisher(self.rabbitmq_config, 'pyloggr.syslog.harvest')
        self._closed_ev = yield self._publisher.start()
        raise Return((self._publisher, self._closed_ev))

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
