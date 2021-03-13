from template.transaction_worker import TransactionWorker
from template.lock_manager import Lock
from template.config import *
class Log:
    def __init__(self, table, *args):
        # transactionNumber = [TransactionWorker1, 2, 3, 4 ... 8]
        # self.information = [] #info map
        # eg. information = [Class Tn --- [[0,0,0,0,0], status]]
        # status = Start or Commit
        self.table = table
        self.args = args
        self.information = {}
        # [key - {args0, status}{args1, status}]
        self.currentWorking = []
        self.logID = 0

    def readLog(self, *args):
        key = args[0]
        tempInfo = []
        if key not in self.information:
            return
        for i in range(len(self.information[key]), 0, -1):
            if self.information[key][i].status is LOG_COMMIT:
                tempInfo.append(self.information[key][i])
                break
        self.currentWorking.append(key)
        self.currentWorking[key].append(tempInfo)
        return True
    
    def writeLog(self, *args):
        key = args[0][0]
        tempArgs = args[0][1:]
        tempStatus = LOG_START
        tempInfo = []
        tempInfo.append(tempArgs)
        tempInfo.append(tempStatus)
        if key in self.information:
            self.information[key].append(tempInfo)
        else:
            self.information.update({key : tempInfo})
        return True
    
    def commitLog(self, args):
        key = args[0]
        if len(args)==3 and isinstance(args[2], list):
            record = self.table.page_directory.get(self.table.index.locate(args[1], key)[0])
            Lock().releaseLock(LOCK_SHARED, [record])
        else:
            record = self.table.page_directory.get(self.table.index.locate(0, key)[0])
            Lock().releaseLock(LOCK_MUTEX, [record])            
        # print('\n')
        tempArgs = args[1:]
        tempStatus = LOG_COMMIT
        tempInfo = []
        tempInfo.append(tempArgs)
        tempInfo.append(tempStatus)
        if key in self.information:
            self.information[key].append(tempInfo)
        else:
            self.information.update({key : tempInfo})
            
        return True
    
    def rollBack(self, args):
        key = args[0]  
        if len(args)==3 and isinstance(args[2], list):
            record = self.table.page_directory.get(self.table.index.locate(args[1], key)[0])
            Lock().releaseLock(LOCK_SHARED, [record])
        else:
            record = self.table.page_directory.get(self.table.index.locate(0, key)[0])
            Lock().releaseLock(LOCK_MUTEX, [record])          
        if len(self.currentWorking) == 0:
            return
        if key not in self.information:
            return
        self.currentWorking.remove(key)
        for i in range(len(self.information[key]), 0, -1):
            if self.information[key][i].status is LOG_COMMIT:
                break
            else:
                self.information[key].remove(i)
        
        return


# functions:

# 1. read Log:
#     read permanent file into memory
#     return true while success

# 2. write log:
#     when a transaction commit (called in transaction class)
#     the last log state = commit
#     write the current in memory log file into file disk
#     read the log file from disk (can skip this step?) to make sure it is the correct latest version
#     return true while success

# 3. roll back:
#     find the latest COMMIT status record in the log
#     return that record

# 4. record history: (necessary ???)
#     create a log for the record,
#     set status of this log to Start
#     append into the log list.