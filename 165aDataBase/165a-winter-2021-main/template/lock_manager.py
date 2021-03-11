from template.config import *
from template.table import Record

class Lock:
    def __init__(self):
        self.unlock = LOCK_UNLOCK
        self.mutex = LOCK_MUTEX
        self.shared = LOCK_SHARED

    def addLock(self, mode, ):
        # detect mutex or shared
        return
    def mutexLock():
        # add mutex lock
        return
    def sharedLock():
        # add shared lock
        return
    def releaseMutexLock():
        # release record mutex lock 
        return
    def releaseSharedLock():
        # release recird mutex lock
        return
    # @param: operation mode
    # @param: record
    def check():
        # detect if any lock
        # return if able to update / read the record
        return




