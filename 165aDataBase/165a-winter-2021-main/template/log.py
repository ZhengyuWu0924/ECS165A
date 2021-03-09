from template.transaction_worker import TransactionWorker
class Log:
    def __init__(self, transaction):
        # transactionNumber = [TransactionWorker1, 2, 3, 4 ... 8]
        # information = [] // one record per line
        # eg. information = [RID, Tn, [0,0,0,0,0]]
        # status = Start or Commit


functions:

1. read Log:
    read permanent file into memory
    return true while success

2. write log:
    when a transaction commit (called in transaction class)
    the last log state = commit
    write the current in memory log file into file disk
    read the log file from disk (can skip this step?) to make sure it is the correct latest version
    return true while success

3. roll back:
    find the latest COMMIT status record in the log
    return that record

4. record history: (necessary ???)
    create a log for the record,
    set status of this log to Start
    append into the log list.