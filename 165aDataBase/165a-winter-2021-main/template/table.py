from template.page import *
from template.index import Index
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, Record_key, columns):
        self.rid = rid
        self.Record_key = Record_key
        self.columns = columns
        
    """
    Return the current RID
    """
    def get_Cur_Rid(self):
        pass
    """
    Get Next available RID
    In Milestone1, next available rid should be 
    current + 1
    """
    def get_Next_AvailableRid(self):
        pass

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, Table_key):
        self.name = name
        self.Table_key = Table_key
        self.num_columns = num_columns
        self.page_directory = {}  ##'key' == 'index': record
        self.index = Index(self)
        # RIDs are shared in one table
        # once a record, take out one RID from the pool
        # MILESTONE1 never put RID back to pool
        # eazy to use BinarySearch in B-tree

        # TODO: remove hard code, and set it in config
        self.total_RID = 100000000
        pass



    """
    Create page ranges for each columns (categories)
    Assign each page range with one base page with nothing in there,
    and write down their location to the page_directory
    For example: A table called UCD, which has 3 columns (categories):
    student name, year, grade. 

    """
    def create_prange(self):
        """
        Pseudo Code:
        Each page rage has two kind of pages
        page[0][x] means BASE PAGE, x is the detail page number
        page[1][y] mean TAIL PAGE, y is the detail page number
        As creating the page, x is depends on the amount of data gonna insert
        y is setted to 0 (which is only 1 page) safe resource
        for all categories
            self.page_directory.append('caretogire[i]' : [page[0][0], page[1][0]])

        """

        pass

    """
    This function should be called at the begging of creating the table
    && the situation that current base page is full and need a new base page 
    """
    def create_base_page(self):
        def

    """
    Create a new tail page and append it to the 
    previous page.
    """
    def create_tail_page(self):
        pass


    def create_record(self):
        pass

    def __merge(self):
        pass
 
