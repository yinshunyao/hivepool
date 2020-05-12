#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/5/12 10:26
# @Author  : ysy
# @Site    : 
# @File    : hive_pool.py
# @Software: PyCharm
from pyhive import hive
import time
try:
    import queue
except ImportError:
    # Python v2
    import Queue as queue
import threading
import logging


class HiveConnection(object):
    def __init__(self, pool,  cursor):
        if not isinstance(pool, HivePool):
            raise Exception("not pool {}".format(type(pool)))

        if not isinstance(cursor, hive.Cursor):
            raise Exception("not hive cursor {}".format(type(cursor)))

        self._pool = pool
        self._cursor = cursor

    @property
    def pool_name(self):
        return self._pool.pool_name

    def __getattr__(self, item):
        """从cursor中获取属性和方法"""
        return getattr(self._cursor, item)

    def close(self):
        try:
            # 连接放回到连接池
            conn = self._cursor._connection
            # todo 需要确认 connection 是否连接正常
            if not isinstance(conn, hive.Connection):
                raise Exception("conn is not hive connection")

            self._pool.add_connection(conn)
        except Exception as e:
            logging.warning("release the cursor error:{}".format(e), exc_info=1)
        finally:
            # 释放cursor
            self._cursor.close()

    def __enter__(self):
        return self._cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class HivePool(object):
    def __init__(self, host, port=None, database=None, pool_size=10, timeout=10, pool_name="", **kwargs):
        self.host = host
        self.port = port
        self.database = database
        self._pool_size = pool_size
        self._kwargs = kwargs
        self._timeout = timeout
        # 连接池和锁
        self._queue_connect = queue.Queue(self._pool_size)
        self._running = 0
        self._connect_lock = threading.RLock()
        self.pool_name = pool_name or "{}:{}/{}".format(host, port or 10000, database)

    def _create_connection(self):
        conn = hive.Connection(host=self.host, port=self.port, database=self.database, **self._kwargs)
        return conn

    @property
    def _size(self):
        return self._queue_connect.qsize() + self._running

    def get_connection(self):
        with self._connect_lock:
            if self._running >= self._pool_size:
                raise Exception("have not idle connect")

            # 如果空
            if self._queue_connect.empty():
                conn = self._create_connection()
            else:
                conn = self._queue_connect.get(timeout=self._timeout)
            if not isinstance(conn, hive.Connection):
                raise Exception("item in pool is not hive connection")

            self._running += 1

        # todo 待判断conn是否连接正常
        return HiveConnection(self, conn.cursor())

    def add_connection(self, conn):
        with self._connect_lock:
            if not isinstance(conn, hive.Connection):
                raise Exception("cannot add connection because the type is {}".format(type(conn)))

            if self._queue_connect.full():
                # 如果队列慢，释放掉
                conn.close()
                raise Exception("hive pool {} is full".format(self.pool_name))

            self._running -= 1
            self._queue_connect.put(conn, timeout=10)

    def close(self):
        """关闭连接池"""
        with self._connect_lock:
            while not self._queue_connect.empty():
                conn = self._queue_connect.get(timeout=self._timeout)
                if not isinstance(conn, hive.Connection):
                    continue

                try:
                    conn.close()
                except Exception as e:
                    logging.error("release hive connection error:{}".format(e), exc_info=1)

    def open(self) -> HiveConnection:
        return self.get_connection()


class _HivePoolManager(object):
    def __init__(self):
        self._manager_lock = threading.RLock()
        self._pools = {}

    def get_hive_pool(self, host, port=10000, database="guotie", timeout=3, **kwargs):
        key = (host, port, database)
        with self._manager_lock:
            if (host, port, database) not in self._pools.keys():
                self._pools[key] = HivePool(*key, timeout=timeout)
            else:
                pass

            return self._pools[key]


_lock_get_hive_manager = threading.RLock()


def _get_hive_manager() -> _HivePoolManager:
    """生成连接池的统一管理"""
    # 写判断加锁
    with _lock_get_hive_manager:
        if "HivePoolManagerInstance" not in globals():
            globals()["HivePoolManagerInstance"] = _HivePoolManager()

    return globals()["HivePoolManagerInstance"]


def get_hive_pool(host, port, database, timeout=3, **kwargs) -> HivePool:
    hive_manager = _get_hive_manager()
    return hive_manager.get_hive_pool(host, port=port, database=database, timeout=timeout, **kwargs)
