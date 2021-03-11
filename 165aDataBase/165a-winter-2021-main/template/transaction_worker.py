from template.table import Table, Record
from template.index import Index
import threading
class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = transactions
        self.result = 0
        self.threads = []
        pass

    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

    """
    Runs a transaction
    """
    def run(self):
        thread = threading.Thread(target=self.run_)
        thread.start()
        # thread.join()
    
    def run_(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
            # thread = threading.Thread(target=func, args=args)
            # self.threads.append(thread)
            # thread.start()
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))


