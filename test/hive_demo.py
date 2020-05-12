#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/5/12 12:12
# @Author  : ysy
# @Site    : 
# @File    : hive_demo.py
# @Software: PyCharm
from hivepool.hive_pool import get_hive_pool, HivePool
import threading
import time
import uuid
import random


host = "cdh070"
port = 10000
database = "guotie"


def query(sleep=0):
    hive_pool = get_hive_pool(host, port=port, database=database, timeout=3)
    u = uuid.uuid1()
    print("{} start".format(u))
    with hive_pool.open() as cursor:
        print(u, "enter")
        cursor.execute("select * from test_hive_alarm_id")
        result = cursor.fetchone()
        print(u, result)
        if not sleep:
            return

        time.sleep(random.randrange(2, 4))
    print(u, "exit")


def multi_query():

    for i in range(20):
        t = threading.Thread(target=query, args=(10, ))
        t.start()


if __name__ == '__main__':
    multi_query()