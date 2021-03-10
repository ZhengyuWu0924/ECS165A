# -* -coding:utf-8 -*-
# @Time:2021/3/8 1:29 上午
# @Author: Tedder Lao


import threading


class LockingCounter():

    def __init__(self):
        self.lock = threading.Lock()
        self.count = 0

    def increment(self):
        with self.lock:
            self.count += 1



