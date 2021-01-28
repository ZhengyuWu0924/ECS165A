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
        
    def get_Cur_Rid(self):
        pass
    def get_Next_AvailableRid():
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
        pass
    # RID --- prange | page | offset
    def create_prange(self, page_key):
        # page = Page(page_key)
        #

        pass

    def create_record(self):
        pass

    def __merge(self):
        pass
 
