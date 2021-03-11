from template.config import *
# from template.table import Record

class Lock:
    def __init__(self):
        self.unlock = LOCK_UNLOCK
        self.mutex = LOCK_MUTEX
        self.shared = LOCK_SHARED

    def addLock(self, mode, record):
        # detect mutex or shared
        status = False
        if len(record) == 0:
            return status
        if mode == LOCK_MUTEX:
            status = self._mutexLock(record[0])
        if mode == LOCK_SHARED:
            status = self._sharedLock(record[0])
        return status
    
    # func: add mutex lock 
    def _mutexLock(self, record):
        # record has already been locked
        if record.lock_amt > 0:
            return False
        # set mutex lock and increase amount
        record.lock_mode = LOCK_MUTEX
        record.lock_amt += 1
        return True
    
    # func: add shared lock 
    def _sharedLock(self, record):
        if record.lock_amt > 0:
            if record.lock_mode == LOCK_MUTEX:
                return False
            else:
                record.lock_amt += 1
        record.lock_mode = LOCK_SHARED
        record.lock_amt += 1
        return True

    # func: release a lock 
    def releaseLock(self, mode, record):
        status = False
        if len(record) == 0:
            return status
        if mode == LOCK_MUTEX:
            status = self._releaseMutexLock(record[0])
        elif mode == LOCK_SHARED:
            status = self._releaseSharedLock(record[0])
        return status
    
    # func: release mutex lock for a record
    def _releaseMutexLock(self, record):
        if record.lock_mode != LOCK_MUTEX:
            return False
        if record.lock_amt == 0:
            return False
        record.lock_mode = LOCK_UNLOCK
        record.lock_amt = 0
        return True
    
    # func: release shared lock for a record
    def _releaseSharedLock(self, record):
        if record.lock_mode != LOCK_SHARED:
            return False
        record.lock_amt -= 1
        if record.lock_amt == 0:
            record.lock_mode = LOCK_UNLOCK
        
        return True
    
    # func: detect if any lock
    # @param: operation mode
    # @param: record
    def check(self, mode, record):
        if mode == LOCK_MUTEX:
            if record[0].lock_mode == LOCK_UNLOCK and record[0].amt == 0:
                return True
            
        if mode == LOCK_SHARED:
            if record[0].lock_mode != LOCK_MUTEX:
                return True
            
        return False




