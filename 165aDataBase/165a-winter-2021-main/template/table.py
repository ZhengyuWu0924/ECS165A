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
        self.tpage_num = 1
    
    def append_page(self, page_pos):
        if page_pos == 0:
            if self.bpage_num == MAX_PAGE_NUM:
                return -1
            page = Page(page_pos) # modified 1935
            self.b_page.append(page)
            self.bpage_num += 1
            # print('27',self.bpage_num)
        if page_pos == 1:
            self.t_page.append(page)
            self.tpage_num += 1


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
    :TODO
    Return the current RID
    """
    def get_Cur_Rid(self):
        pass
    """
    :TODO
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
        self.free_brid = 0
        self.free_trid = 0
        self.rid_list = []
        # RIDs are shared in one table
        # once a record, take out one RID from the pool
        # MILESTONE1 never put RID back to pool
        # eazy to use BinarySearch in B-tree

        # TODO: remove hard code, and set it in config
        # self.total_RID = 100000000
        pass

    # b_page and t_page will have seperate rids
    def next_free_rid(self, page_pos):
        if page_pos == 0:
            rid = 'b' + str(self.free_brid)
            # if rid >=100000000:
            #     return -1   
            self.free_brid = self.free_brid + 1
        if page_pos == 1: 
            rid = 't' + str(self.free_trid)
            self.free_trid += 1
        return rid


    """
    Create page ranges for each columns (categories)
    Assign each page range with one base page with nothing in there,
    and write down their location to the page_directory
    For example: A table called UCD, which has 3 columns (categories):
    student name, year, grade. 

    """
    # To Do: build index during looping
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
            prange = Prange(b_page, t_page, self.prange_num)
            # tree = Index(self)
            # self.index.append(tree)
            self.page_directory.update({i: [prange]})
            # ZYW: add a return statement to return the prange? 
        pass
    
    def add_prange(self, column):
        b_page = Page(0)
        t_page = Page(0)
        prange = Prange(b_page, t_page, self.prange_num)
        self.page_directory.get(column).append(prange)

    def create_record(self, rid, indirect, value, data, first):
        record = Record(rid, indirect, value, data)
        if first == True:
            self.record_directory.update({rid: record, value: record})
            self.rid_list.append(rid)
        return record

    # To Do: inset record to index
    def insert_record(self, *data):
        #check if latest prange is full
        record = None 
        first = None
        rid = None
        # if self.page_directory.get(0)[-1].bpage_num == MAX_PAGE_NUM:
        #     print('reaching max')
        #     self.prange_num += 1
        #     self.free_brid = 0
        #     self.free_trid = 0
        # rid = self.next_free_rid(0)
        # record = Record(rid, rid, data[0], data)
        # self.record_directory.update({record.rid: record, record.Record_key: record})
        # self.rid_list.append(record.rid)
        # record.indirect = record.rid
        for i in range(self.num_columns):
            if i == 0:
                first = True
            else:
                first = False

            if self.page_directory.get(i)[-1].b_page[-1].has_capacity() == True:
                if first == True:
                    rid = self.next_free_rid(0)
                record = self.create_record(rid, rid, data[i], data, first)
                prange = self.page_directory.get(i)[-1]
                record.offset = prange.b_page[-1].writeRecord(data[i])
                record.page_pos = len(prange.b_page) - 1
                record.prange_pos = self.prange_num
            else:
                if self.insert_page_to(i) == -1:
                    self.add_prange(i)
                    if first == True:
                        print('adding prange')
                        self.prange_num += 1
                        rid = self.next_free_rid(0)
                    # self.free_brid = 0
                    self.free_trid = 0
                    
                record = self.create_record(rid, rid, data[i], data, first)
                prange = self.page_directory.get(i)[-1]
                record.offset = prange.b_page[-1].writeRecord(data[i])
                record.page_pos = len(prange.b_page) - 1
                record.prange_pos = self.prange_num
            self.index.insert(i, data[i], record.rid)
        return True

    # if key does not exist then return false
    # To Do: update record to index
    def update_record(self, *data):
        std_id = data[0]
        rid = self.next_free_rid(1)
        # print(rid)
        base_record = self.record_directory.get(std_id)
        if base_record == None:
            return False
        # get current prange position
        cur_prange_pos = base_record.prange_pos
        prev_record = self.record_directory.get(base_record.indirect)
        cur_record = Record(rid, prev_record.indirect, data[0], data)
        base_record.indirect = cur_record.rid
        cur_record.indirect = prev_record.rid
        self.record_directory.update({cur_record.rid: cur_record})
        for i in range(self.num_columns):
            prev_data = self.get_data(prev_record.rid, i, prev_record.prange_pos, prev_record.page_pos, prev_record.offset)
            if prev_data == '/':
                # self.index.update(i, None, data[i], base_record.rid)
                prev_data = None
            # print('prev_data', prev_data)
            # self.index.update(i, prev_data, data[i], base_record.rid)
            data_ = data[i]
            # handle the case when the data is empty, we will emerge data
            # from previous record
            if data[i] == None:
                data_ = prev_data
                # handle the case when the data is always empty, we use '/' to
                # represent the final value
                if data_ == '/':
                    data_ = None
                # print('176',prev_record.rid, prev_record.prange_pos, prev_record.page_pos, data_)
            if self.page_directory.get(i)[cur_prange_pos].t_page[-1].has_capacity() == True:
                prange = self.page_directory.get(i)[cur_prange_pos]
                cur_record.offset = prange.t_page[-1].writeRecord(data_)
                cur_record.page_pos = len(prange.t_page) - 1
                cur_record.prange_pos = cur_prange_pos
            else:
                self.update_page_to(i, cur_prange_pos)
                prange = self.page_directory.get(i)
                cur_record.offset = prange.t_page[-1].writeRecord(data_)
                cur_record.page_pos = len(prange.t_page) - 1
                cur_record.prange_pos = cur_prange_pos
            # print("teble 237", i, prev_data, data_, base_record.rid)
            self.index.update(i, prev_data, data_, base_record.rid)
        return 0

    def insert_page_to(self, ith_column):
        prange = self.page_directory.get(ith_column)[-1]
        if prange.append_page(0) == -1:
            return -1  

    def get_rid_list(self):
        return self.rid_list

    def update_page_to(self, ith_column, ith_prange):
        prange = self.page_directory.get(ith_column)[ith_prange]
        prange.append_page(1)

    def get_data(self, rid, ith_col, prange_pos, page_pos, offset):
        if rid[0] == 'b':
            page = self.page_directory.get(ith_col)[prange_pos].b_page[page_pos]
            return page.readRecord(offset)
        page = self.page_directory.get(ith_col)[prange_pos].t_page[page_pos]
        # print(page.readRecord(offset))
        return page.readRecord(offset)

    def read_record(self, std_id):
        record = self.record_directory.get(std_id)
        rid = record.indirect
        # print(rid)
        record = self.record_directory.get(rid)
        offset = record.offset
        page_pos = record.page_pos
        prange_pos = record.prange_pos
        res = []
        for i in range(self.num_columns):
            if rid[0] == 'b':
                page = self.page_directory.get(i)[prange_pos].b_page[page_pos]
                data = page.readRecord(offset)
                res.append(data)
            else:
                page = self.page_directory.get(i)[prange_pos].t_page[page_pos]
                data = page.readRecord(offset)
                res.append(data)
        return res

    def __merge(self):
        pass
    

