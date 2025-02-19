from template.table import Table, Record
from template.index import Index
from template.log import Log
from template.config import *
import threading

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.table = None
        self.log = None
        self.sem = threading.RLock()

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args, table = None):
        if self.table is None and table is not None:
            self.table = table
        # if len(args)==3 and isinstance(args[2], list):
        #     print('selecting')   
        self.queries.append((query, args))
        self.log = Log(table, args)

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        # self.sem.acquire()
        # print(len(self.queries))
        for query, args in self.queries:
            # print(query, args)
            result = query(*args)
            self.log.writeLog(args)
            # If the query has failed the transaction should abort
            # print(result)
            if result == False:  
                # print('---aborting', threading.current_thread(), args,'----')
                # self.sem.release()
                return self.abort(args)
        # self.sem.release()
        return self.commit()

    def abort(self, args):
        #TODO: do roll-back and any other necessary operations
        # print(args)
        self.log.rollBack(args)
        return False

    def commit(self):
        # self.sem.acquire()
        for _, args in self.queries:
            # print(threading.currentThread())
            # print(self.table.rid_list)
            self.log.commitLog(args)
        # self.sem.release()
        
        return True


