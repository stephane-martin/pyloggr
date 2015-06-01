# encoding: utf-8

"""
Small wrapper around lmdb

"""
__author__ = 'stef'

from itertools import imap

import ujson
import lmdb
from cytoolz import valmap
from future.utils import viewitems

from pyloggr.utils import to_unicode, to_bytes


class LmdbWrapper(object):
    opened_db = dict()

    def __init__(self, path):
        self.path = path
        self.env = None

    def open(self):
        # noinspection PyArgumentList
        self.env = lmdb.Environment(
            path=self.path, map_size=10485760, max_dbs=0, max_spare_txns=1
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

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @classmethod
    def get_instance(cls, path):
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
        return ujson.loads(obj_bytes)

    def get_many_objs(self, keys):
        with self.env.begin() as txn:
            objs_bytes = {
                key: txn.get(to_bytes(key))
                for key in keys
            }
        return valmap(
            lambda obj_byte: None if obj_byte is None else ujson.loads(obj_byte),
            objs_bytes
        )

    def put_obj(self, key, obj):
        key = to_bytes(key)
        obj_bytes = to_bytes(ujson.dumps(obj))
        with self.env.begin(write=True) as txn:
            result = txn.put(key, obj_bytes, overwrite=True)
        return result

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
            txn.put(obj_idx, obj_bytes)
            txn.put(end_keyname, str(end_idx + 1), overwrite=True)
            if start_idx is None:
                txn.put(start_keyname, "0", overwrite=True)

    def rpush_many(self, queue_name, objs):
        queue_name = "__queue__" + to_bytes(queue_name)
        start_keyname = queue_name + "__start"
        end_keyname = queue_name + "__end"
        with self.env.begin(write=True) as txn:
            start_idx = txn.get(start_keyname)
            end_idx = int(txn.get(end_keyname, default=-1))
            last = None
            for i, obj in enumerate(objs):
                obj_idx = queue_name + '__' + str(end_idx + 1 + i)
                txn.put(obj_idx, to_bytes(ujson.dumps(obj)))
                last = i
            if last:
                txn.put(end_keyname, str(end_idx + 1 + last), overwrite=True)
                if start_idx is None:
                    txn.put(start_keyname, "0", overwrite=True)

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
            return ujson.loads(obj_bytes)
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
        # todo: take care of JSON errors
        return [ujson.loads(obj_bytes) for obj_bytes in objs_bytes if obj_bytes]

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
