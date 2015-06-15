# encoding: utf-8

"""
Small wrapper around lmdb
"""
__author__ = 'stef'

import logging
import time

# noinspection PyCompatibility
from concurrent.futures import ThreadPoolExecutor, Future
from tornado.ioloop import IOLoop
import ujson
import lmdb
from future.utils import viewitems

from pyloggr.utils import to_bytes, to_unicode


def _json_decode(obj_bytes):
    if obj_bytes is None:
        return None
    try:
        return ujson.loads(obj_bytes)
    except (ValueError, TypeError):
        logging.getLogger(__name__).exception("lmdb_wrapper: JSON decoding error: {}".format(
            to_unicode(obj_bytes)
        ))
        return None


def _dumps(obj):
    if hasattr(obj, 'dumps'):
        return to_bytes(obj.dumps())
    else:
        return to_bytes(ujson.dumps(obj))


class LmdbWrapper(object):
    """
    Wrapper around lmdb that eases storage and retrieval of JSONable python objects
    """
    opened_db = dict()

    def __init__(self, path, size=52428800):
        self.path = path
        self.env = None
        self.size = size

    def open(self, sync=True, metasync=True, lock=True, max_dbs=10, max_spare_txns=10):
        if not self.env:
            # noinspection PyArgumentList
            self.env = lmdb.Environment(
                path=self.path, map_size=self.size, max_dbs=max_dbs, sync=sync, metasync=metasync,
                lock=lock, max_spare_txns=max_spare_txns
            )
            self.opened_db[self.path] = self
        return self

    def close(self):
        if self.env:
            self.env.close()
            self.env = None
            if self.path in self.opened_db:
                del self.opened_db[self.path]

    def __enter__(self):
        return self.open()

    # noinspection PyUnusedLocal
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @classmethod
    def get_instance(cls, path):
        """
        :param path: database directory name
        :type path: str
        :return: LmdbWrapper object
        :rtype: LmdbWrapper
        """
        if path not in cls.opened_db:
            cls.opened_db[path] = cls(path).open()
        return cls.opened_db[path]

    @classmethod
    def close_all(cls):
        for db in cls.opened_db.values():
            db.close()

    def get_obj(self, key):
        key = to_bytes(key)
        with self.env.begin() as txn:
            obj_bytes = txn.get(key)
        if obj_bytes is None:
            return None
        return _json_decode(obj_bytes)

    def __getitem__(self, item):
        return self.get_obj(item)

    def __setitem__(self, key, value):
        self.put_obj(key, value)

    def queue(self, queue_name):
        return Queue(queue_name, self)

    def hash(self, hash_name):
        return Hash(hash_name, self)

    def set(self, set_name):
        return Set(set_name, self)

    def get_many_objs(self, keys):
        with self.env.begin() as txn:
            objs_bytes = {
                key: txn.get(to_bytes(key))
                for key in keys
            }
        return {
            key: None if obj_bytes is None else _json_decode(obj_bytes)
            for key, obj_bytes in viewitems(objs_bytes)
        }

    def put_obj(self, key, obj):
        key = to_bytes(key)
        with self.env.begin(write=True) as txn:
            return txn.put(key, _dumps(obj), overwrite=True)

    def put_many_objs(self, d):
        with self.env.begin(write=True) as txn:
            return {
                key: txn.put(to_bytes(key), _dumps(obj), overwrite=True)
                for key, obj in viewitems(dict(d))
            }

    def del_obj(self, key):
        key = to_bytes(key)
        with self.env.begin(write=True) as txn:
            return txn.delete(key)

    def del_many_obj(self, keys):
        with self.env.begin(write=True) as txn:
            return {
                key: txn.delete(to_bytes(key))
                for key in keys
            }

    def add_to_set(self, set_name, item):
        item = to_unicode(item)
        idx = "__set__" + to_bytes(set_name)
        with self.env.begin(write=True) as txn:
            existing_items = txn.get(idx)
            if existing_items is None:
                txn.put(idx, ujson.dumps([item]))
            else:
                existing_items = ujson.loads(existing_items)
                if item not in existing_items:
                    existing_items.append(item)
                    txn.put(idx, ujson.dumps(existing_items))

    def remove_from_set(self, set_name, item):
        item = to_unicode(item)
        idx = "__set__" + to_bytes(set_name)
        with self.env.begin(write=True) as txn:
            existing_items = txn.get(idx)
            if existing_items is not None:
                existing_items = ujson.loads(existing_items)
                if item in existing_items:
                    existing_items.remove(item)
                    txn.put(idx, ujson.dumps(existing_items))

    def is_in_set(self, set_name, item):
        item = to_unicode(item)
        idx = "__set__" + to_bytes(set_name)
        with self.env.begin() as txn:
            existing_items = txn.get(idx)
            if existing_items is None:
                return False
            existing_items = ujson.loads(existing_items)
            return item in existing_items

    def card_of_set(self, set_name):
        idx = "__set__" + to_bytes(set_name)
        with self.env.begin() as txn:
            existing_items = txn.get(idx)
            if existing_items is None:
                return 0
            return len(ujson.loads(existing_items))

    def members_of_set(self, set_name):
        idx = "__set__" + to_bytes(set_name)
        with self.env.begin() as txn:
            existing_items = txn.get(idx)
            if existing_items is None:
                return set()
            return set(ujson.loads(existing_items))


class Set(object):
    def __init__(self, set_name, wrapper):
        self.set_name = to_bytes(set_name)
        self.wrapper = wrapper

    def add(self, key):
        self.wrapper.add_to_set(self.set_name, key)

    def remove(self, key):
        self.wrapper.remove_from_set(self.set_name, key)

    def card(self):
        return self.wrapper.card_of_set(self.set_name)

    def __len__(self):
        return self.wrapper.card_of_set(self.set_name)

    def members(self):
        return self.wrapper.members_of_set(self.set_name)

    def __contains__(self, key):
        return self.wrapper.is_in_set(self.set_name, key)


class Hash(object):
    def __init__(self, hash_name, wrapper):
        self.hash_name = to_bytes(hash_name)
        self.wrapper = wrapper

    def __getitem__(self, item):
        key = "__hash__" + self.hash_name + "__" + to_bytes(item)
        return self.wrapper.get_obj(key)

    def __setitem__(self, item, value):
        key = "__hash__" + self.hash_name + "__" + to_bytes(item)
        self.wrapper.put_obj(key, value)

    def __delitem__(self, item):
        key = "__hash__" + self.hash_name + "__" + to_bytes(item)
        self.wrapper.del_obj(key)


class Queue(object):
    def __init__(self, queue_name, wrapper):
        """
        :type wrapper: LmdbWrapper
        """
        self.queue_name = to_bytes(queue_name)
        self.subdbname = "__queue__" + self.queue_name
        self.subdb = wrapper.env.open_db(key=self.subdbname, txn=None, reverse_key=False, dupsort=False, create=True)
        self.env = wrapper.env
        self.wrapper = wrapper
        self.exe = None

    def __enter__(self):
        self.exe = ThreadPoolExecutor(max_workers=10)
        return self

    def __exit__(self, type, value, traceback):
        if self.exe is not None:
            self.exe.shutdown()

    def generator(self, exclude=None):
        if exclude is None:
            exclude = set()
        with self.env.begin(db=self.subdb, write=False) as txn:
            with txn.cursor(db=self.subdb) as c:
                if not c.first():
                    return
                for key, value in c.iternext(keys=True, values=True):
                    if key in exclude:
                        continue
                    if key:
                        if value is not None:
                            obj = _json_decode(value)
                            if obj:
                                yield (key, obj)

    def push(self, obj, idx=None):
        idx = to_bytes(obj.lmdb_idx()) if idx is None else to_bytes(idx)
        with self.env.begin(db=self.subdb, write=True) as txn:
            idx = to_bytes(idx)
            if hasattr(obj, 'dumps'):
                obj_bytes = to_bytes(obj.dumps())
            else:
                obj_bytes = to_bytes(ujson.dumps(obj))
            result = txn.put(idx, obj_bytes, overwrite=True)
        return result

    def delete(self, obj=None, idx=None):
        if obj is None and idx is None:
            return None
        idx = to_bytes(obj.lmdb_idx()) if idx is None else to_bytes(idx)
        with self.env.begin(db=self.subdb, write=True) as txn:
            return txn.delete(idx, db=self.subdb)

    def lpop(self):
        with self.env.begin(db=self.subdb, write=True) as txn:
            with txn.cursor(db=self.subdb) as c:
                if c.first():
                    idx = c.key()
                    obj_bytes = c.pop(idx)
                    if obj_bytes is None:
                        return None
                    else:
                        obj = _json_decode(obj_bytes)
                        if obj is None:
                            return None
                        else:
                            return idx, obj

                else:
                    # queue is empty
                    return None

    def pop(self, key=None):
        if key is None:
            return self.lpop()
        key = to_bytes(key)
        with self.env.begin(db=self.subdb, write=True) as txn:
            obj_bytes = txn.pop(key)
            return _json_decode(obj_bytes)

    def pop_all(self):
        results = []
        with self.env.begin(db=self.subdb, write=True) as txn:
            with txn.cursor(db=self.subdb) as c:
                if not c.first():
                    return []
                for key in c.iternext(keys=True, values=False):
                    obj = _json_decode(c.pop(key))
                    if obj:
                        results.append((key, obj))
        return results

    def empty(self):
        with self.env.begin(db=self.subdb, write=False) as txn:
            with txn.cursor(db=self.subdb) as c:
                return not c.first()

    def extend(self, values):
        with self.env.begin(db=self.subdb, write=True) as txn:
            results = [txn.put(to_bytes(obj.lmdb_idx()), to_bytes(ujson.dumps(obj)), overwrite=True) for obj in values]
        return results

    def keys(self):
        with self.env.begin(db=self.subdb) as txn:
            with txn.cursor(db=self.subdb) as c:
                if not c.first():
                    return set()
                return set(c.iternext(keys=True, values=False))


    def wait_not_empty_future(self, timeout=None, tick=1.0):
        f = Future()
        exe = None
        if self.exe is None:
            exe = ThreadPoolExecutor(max_workers=10)

        def _wait_until_not_empty():
            while self.empty():
                print('empty')
                time.sleep(tick)

        def _nothing(g=None):
            if not f.done():
                f.set_result(None)
            if exe is not None:
                exe.shutdown(wait=False)

        if timeout is not None:
            IOLoop.current().call_later(int(timeout), _nothing)
        if self.exe is None:
            wait_until_f = exe.submit(_wait_until_not_empty)
        else:
            wait_until_f = self.exe.submit(_wait_until_not_empty)

        IOLoop.current().add_future(wait_until_f, _nothing)
        return f

    def wait_not_empty(self, timeout=None, tick=1.0, exclude=None):
        start = time.time()
        if exclude is None:
            while self.empty():
                if timeout is not None:
                    if (time.time() - start) >= timeout:
                        return False
                time.sleep(tick)
            return True
        else:
            while True:
                if timeout is not None:
                    if (time.time() - start) >= timeout:
                        return False
                if not self.empty():
                    if len(self.keys().difference(exclude)) > 0:
                        return True
                time.sleep(tick)
