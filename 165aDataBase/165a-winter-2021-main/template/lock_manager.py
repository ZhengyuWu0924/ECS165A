from template.config import *
from template.table import Record

class Lock:
    def __init__(self):
        self.unlock = LOCK_UNLOCK
        self.mutex = LOCK_MUTEX
        self.shared = LOCK_SHARED

    def addLock(self, mode, *record):
        # detect mutex or shared
        return
    def _mutexLock(self, *record):
        # add mutex lock
        return
    def _sharedLock(self, *record):
        # add shared lock
        return
    def releaseLock(self, mode, *record):
        return
    def _releaseMutexLock(self, *record):
        # release record mutex lock 
        return
    def _releaseSharedLock(self, *record):
        # release recird mutex lock
        return
    # @param: operation mode
    # @param: record
    def check(self, *record):
        # detect if any lock
        # return if able to update / read the record
        return




