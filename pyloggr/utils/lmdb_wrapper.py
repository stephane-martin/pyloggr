# encoding: utf-8

"""
Small wrapper around lmdb
"""
__author__ = 'stef'

import logging

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


class LmdbWrapper(object):
    """
    Wrapper around lmdb that eases storage and retrieval of JSONable python objects
    """
    opened_db = dict()

    def __init__(self, path, size=52428800):
        self.path = path
        self.env = None
        self.size = size

    def open(self):
        if not self.env:
            # noinspection PyArgumentList
            self.env = lmdb.Environment(
                path=self.path, map_size=self.size, max_dbs=0, max_spare_txns=3
            )
            self.opened_db[self.path] = self
        return self

    def close(self):
        if self.env:
            self.env.close()
            self.env = None
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
        obj_bytes = to_bytes(ujson.dumps(obj))
        with self.env.begin(write=True) as txn:
            return txn.put(key, obj_bytes, overwrite=True)

    def put_many_objs(self, d):
        with self.env.begin(write=True) as txn:
            return {
                key: txn.put(to_bytes(key), to_bytes(ujson.dumps(obj)), overwrite=True)
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

    def rpush(self, queue_name, obj):
        queue_name = to_bytes("__queue__" + queue_name)
        start_keyname = queue_name + "__start"
        end_keyname = queue_name + "__end"
        obj_bytes = to_bytes(ujson.dumps(obj))
        with self.env.begin(write=True) as txn:
            start_idx = txn.get(start_keyname)
            end_idx = int(txn.get(end_keyname, default=-1))
            obj_idx = queue_name + '__' + str(end_idx + 1)
            txn.put(end_keyname, str(end_idx + 1), overwrite=True)
            if start_idx is None:
                txn.put(start_keyname, "0", overwrite=True)
            return txn.put(obj_idx, obj_bytes)

    def rpush_many(self, queue_name, objs):
        queue_name = "__queue__" + to_bytes(queue_name)
        start_keyname = queue_name + "__start"
        end_keyname = queue_name + "__end"
        with self.env.begin(write=True) as txn:
            start_idx = txn.get(start_keyname)
            end_idx = int(txn.get(end_keyname, default=-1))
            results = [
                txn.put(
                    queue_name + '__' + str(end_idx + 1 + i),
                    to_bytes(ujson.dumps(obj))
                )
                for i, obj in enumerate(objs)
            ]
            if len(results):
                txn.put(end_keyname, str(end_idx + len(results)), overwrite=True)
                if start_idx is None:
                    txn.put(start_keyname, "0", overwrite=True)
        return results.count(True)

    def lpop(self, queue_name):
        queue_name = "__queue__" + to_bytes(queue_name)
        start_keyname = queue_name + "__start"
        end_keyname = queue_name + "__end"
        with self.env.begin(write=True) as txn:
            start_idx = txn.get(start_keyname)
            end_idx = txn.get(end_keyname)
            if start_idx is None or end_idx is None:
                return None
            obj_idx = queue_name + '__' + start_idx
            obj_bytes = txn.pop(obj_idx)
            start_idx = int(start_idx)
            end_idx = int(end_idx)
            if start_idx == end_idx:
                # no more object in the queue
                txn.delete(start_keyname)
                txn.delete(end_keyname)
            else:
                txn.put(start_keyname, str(start_idx + 1), overwrite=True)
        if obj_bytes:
            return _json_decode(obj_bytes)
        return None

    def pop_all(self, queue_name):
        queue_name = "__queue__" + to_bytes(queue_name)
        start_keyname = queue_name + "__start"
        end_keyname = queue_name + "__end"
        with self.env.begin(write=True) as txn:
            start_idx = txn.get(start_keyname)
            end_idx = txn.get(end_keyname)
            if start_idx is None or end_idx is None:
                return None
            start_idx = int(start_idx)
            end_idx = int(end_idx)
            objs_idx = (queue_name + '__' + str(idx) for idx in range(start_idx, end_idx + 1))
            objs_bytes = [txn.pop(idx) for idx in objs_idx]
            txn.delete(start_keyname)
            txn.delete(end_keyname)
        results = (_json_decode(obj_bytes) for obj_bytes in objs_bytes if obj_bytes)
        return [result for result in results if result]

    def queue_length(self, queue_name):
        queue_name = "__queue__" + to_bytes(queue_name)
        start_keyname = queue_name + "__start"
        end_keyname = queue_name + "__end"
        with self.env.begin() as txn:
            start_idx = txn.get(start_keyname)
            end_idx = txn.get(end_keyname)
        if start_idx is None or end_idx is None:
            return 0
        return int(end_idx) - int(start_idx) + 1

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
        self.queue_name = to_bytes(queue_name)
        self.wrapper = wrapper

    def rpush(self, value):
        return self.wrapper.rpush(self.queue_name, value)

    def append(self, value):
        return self.wrapper.rpush(self.queue_name, value)

    def lpop(self):
        return self.wrapper.lpop(self.queue_name)

    def pop_all(self):
        return self.wrapper.pop_all(self.queue_name)

    def __len__(self):
        return self.wrapper.queue_length(self.queue_name)

    def extend(self, values):
        return self.wrapper.rpush_many(self.queue_name, values)

