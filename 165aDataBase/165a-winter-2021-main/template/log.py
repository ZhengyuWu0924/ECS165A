from template.transaction_worker import TransactionWorker
from template.config import *
class Log:
    def __init__(self):
        # transactionNumber = [TransactionWorker1, 2, 3, 4 ... 8]
        information = [] #info map
        # eg. information = [Class Tn --- [[0,0,0,0,0], status]]
        # status = Start or Commit
        # self.status = LOG_START
        

    def readLog(self):
        return True
    
    def writeLog(self):

        return True
    
    def rollBack(self):
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