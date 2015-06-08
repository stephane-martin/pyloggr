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
        if idx is None:
            idx = to_bytes(obj.lmdb_idx())
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
        if idx is not None:
            with self.env.begin(db=self.subdb, write=True) as txn:
                return txn.delete(to_bytes(idx), db=self.subdb)
        with self.env.begin(db=self.subdb, write=True) as txn:
            return txn.delete(to_bytes(obj.lmdb_idx()), db=self.subdb)

    def pop(self):
        return self.lpop()

    def lpop(self):
        with self.env.begin(db=self.subdb, write=True) as txn:
            with txn.cursor(db=self.subdb) as c:
                if c.first():
                    idx, obj_bytes = c.item()
                    if obj_bytes is None:
                        c.delete()
                        return None
                    else:
                        obj = _json_decode(obj_bytes)
                        if obj is None:
                            c.delete()
                            return None
                        else:
                            c.delete()
                            return idx, obj
                else:
                    # queue is empty
                    return None

    def pop_all(self):
        results = []
        with self.env.begin(db=self.subdb, write=True) as txn:
            with txn.cursor(db=self.subdb) as c:
                if not c.first():
                    return []
                for key in c.iternext(keys=True, values=False):
                    value = c.pop(key)
                    if value:
                        obj = _json_decode(value)
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

    def lpop_wait(self, timeout):
        f = Future()
        exe = ThreadPoolExecutor(max_workers=3)

        def _wait_until_not_empty():
            while self.empty():
                time.sleep(1)

        def _maybe_ive_got_an_element(g):
            result = g.result()
            if result is not None:
                f.set_result(result)
                exe.shutdown()
            else:
                wait_until_g = exe.submit(_wait_until_not_empty)
                IOLoop.current().add_future(wait_until_g, _maybe_im_not_empty)

        def _maybe_im_not_empty(g):
            popping_f = exe.submit(self.lpop)
            IOLoop.current().add_future(popping_f, _maybe_ive_got_an_element)

        wait_until_f = exe.submit(_wait_until_not_empty)
        IOLoop.current().add_future(wait_until_f, _maybe_im_not_empty)
        return f
