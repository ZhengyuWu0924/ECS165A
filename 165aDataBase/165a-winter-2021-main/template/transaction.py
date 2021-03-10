from template.table import Table, Record
from template.index import Index
from template.log import Log
from template.config import *

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.table = None
        self.log = None

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args, table = None):
        tempTable = self.table
        if table is not None:
            self.table = table
            
        self.queries.append((query, args))
        tempTable = self.table
        self.log = Log(table, args)

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            result = query(*args)
            self.log.writeLog(args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort(args)
        return self.commit()

    def abort(self, *args):
        #TODO: do roll-back and any other necessary operations
        self.log.rollBack(args)
        return False

    def commit(self):
        # TODO: commit to database
        # call write-to-database function
        # if write to database sucessful 
        # then change state from "Start" to "Commit".
        for _, args in self.queries:
            self.log.commitLog(args)
        
        return True


