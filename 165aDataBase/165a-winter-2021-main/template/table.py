from template.page import *
from template.index import Index
from template.config import *
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Prange:

    def __init__(self, b_page, t_page, prange_id):
        self.b_page = [b_page]
        self.t_page = [t_page]
        self.prange_id = prange_id
        self.bpage_num = 1

    def append_page(self, page_pos):
        if page_pos == 0:
            if self.bpage_num == MAX_PAGE_NUM:
                return -1
            page = Page(self.page_num)
            self.b_page.append(page)
            self.bpage_num += 1
        if page_pos == 1:
            self.t_page.append(page)


class Record:

    def __init__(self, rid, indirect, Record_key, columns):
        self.rid = rid
        self.indirect = indirect
        # self.indirected = None
        # self.schema = schema
        # self.timestamp = timestamp
        self.prange_pos = 0
        self.page_pos = 0
        self.offset = 0
        self.Record_key = Record_key #ex. student_id
        self.columns = columns #tuple of grades
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
        self.record_directory = {}
        self.prange_num = 0
        self.free_rid = 0
        # RIDs are shared in one table
        # once a record, take out one RID from the pool
        # MILESTONE1 never put RID back to pool
        # eazy to use BinarySearch in B-tree

        # TODO: remove hard code, and set it in config
        # self.total_RID = 100000000
        pass

    def next_free_rid(self):
        rid = self.free_rid
        if rid >=100000000:
            return -1
        self.free_rid += 1
        return rid


    """
    Create page ranges for each columns (categories)
    Assign each page range with one base page with nothing in there,
    and write down their location to the page_directory
    For example: A table called UCD, which has 3 columns (categories):
    student name, year, grade. 

    """
    def create(self):
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
        for i in range(self.num_columns):
            b_page = Page(0)
            t_page = Page(0)
            prange = Prange(b_page, t_page, prange_num)
            self.page_directory.update({i: [prange]})
        pass
    
    def insert_record(self, *data):
        record = Record(self.next_free_rid, self.next_free_rid, *data[0], *data)
        self.record_directory.update({record.rid: record, record.Record_key: record})
        for i in range(self.num_columns):
            if self.page_directory.get(i).b_page[-1].has_capacity() == True:
                self.page_directory.get(i).b_page[-1].writeRecord(*data[i])
            else:
                if self.insert_page_to(i) == -1:
                    return -1
                self.page_directory.get(i).b_page[-1].writeRecord(*data[i])
        return 0

    def update_record(self, *data):
        std_id = *data[0]
        base_record = self.record_directory.gwt(std_id)
        prev_record = self.record_directory.get(base_record.indirect)
        cur_record = Record(self.next_free_rid, prev_record.indirect, *data[0], *data)
        base_record.indiret = cur_record.rid
        self.record_directory.update({cur_record.rid: cur_record})
        for i in range(self.num_columns):
            if self.page_directory.get(i).t_page[-1].has_capacity() == True:
                self.page_directory.get(i).t_page[-1].writeRecord(*data[i])
            else:
                update_page_to(i)
                self.page_directory.get(i).t_page[-1].writeRecord(*data[i])
        return 0

    def insert_page_to(self, ith_column):
        prange = self.page_directory.get(ith_column)
        prange.append_page(0)   

    def update_page_to(self, ith_column):
        prange = self.page_directory.get(ith_column)
        prange.append_page(1)

    def create_record(self):
        pass

    def __merge(self):
        pass
 
