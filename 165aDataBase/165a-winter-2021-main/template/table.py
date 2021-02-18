from template.page import *
from template.index import Index
from template.config import *
from time import time
from datetime import datetime

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
            page = Page(page_pos)
            self.t_page.append(page)
            self.tpage_num += 1

class Record:

    def __init__(self, rid, indirect, Record_key, columns):
        # default meta_data_cols
        self.rid = rid
        self.indirect = indirect
        self.schema = int('1' * len(columns)) #1 means unchanged, 2 means changed
        self.timestamp = int(time())
        # data pos info
        self.prange_pos = 0
        self.page_pos = 0
        self.offset = 0
        self.Record_key = Record_key #ex. student_id
        # origin data
        self.columns = columns #tuple of grades
        # latest data
        self.columns_ = columns
    
    def get_meta(self):
        # print(datetime.fromtimestamp(self.timestamp))
        # print(self.timestamp)
        temp_rid = self.rid
        temp_ind = self.indirect
        if self.rid[0] == 'b':
            temp_rid = temp_rid.replace('b','1')
        else:
            temp_rid = temp_rid.replace('t','2')

        if self.indirect[0] == 'b':
            temp_ind = temp_ind.replace('b','1')
        else:
            temp_ind = temp_ind.replace('t','2')
        # print(int(temp_rid), int(temp_ind))
        return [int(temp_rid), int(temp_ind), self.schema, self.timestamp]
    

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
        self.prange_directory = {}  #'col_num': 'page_range_list'
        self.page_directory = {}    #'RID': 'record obj'
        self.origin_base_page_memory = [] # original unmerged and 
        self.after_merge_base_page_memory = {} # the new copy that is being merged
        self.index = Index(self)
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
    def get_prange_num(self):
        return self.prange_num
    
    def load_basepage_copy_to_memory(self,column):
        b_page = Page(0)
        self.origin_base_page_memory.append(b_page)

    def sort_tailpage_reverse(self,column):
        t_page = Page(0)
        tpage.sort(reverse=False)

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
        for i in range(self.num_columns + META_DATA_COL_NUM):
            b_page = Page(0)
            t_page = Page(0)
            prange = Prange(b_page, t_page, self.prange_num)
            # tree = Index(self)
            # self.index.append(tree)
            self.prange_directory.update({i: [prange]})
            # ZYW: add a return statement to return the prange? 

    # def insert_meta_data(self, prange, record):
    #     prange.b_page[record.page_pos].
    
    def add_prange(self, column):
        b_page = Page(0)
        t_page = Page(0)
        prange = Prange(b_page, t_page, self.prange_num)
        self.prange_directory.get(column).append(prange)

    def create_record(self, rid, indirect, value, data, first):
        # print('indirect', indirect)
        # if indirect == None:
            # print('empty', rid)
        record = Record(rid, indirect, value, data)
        if first == True:
            self.page_directory.update({rid: record, value: record})
            self.rid_list.append(rid)
        return record

    # To Do: inset record to index
    def insert_record(self, *data):
        #check if latest prange is full
        record = None 
        first = None
        rid = None
        meta_cols = []
        for i in range(self.num_columns + META_DATA_COL_NUM):
            if i == 0:
                first = True
            else:
                first = False

            if self.prange_directory.get(i)[-1].b_page[-1].has_capacity() == True:
                if i < self.num_columns:
                    if first == True:
                        rid = self.next_free_rid(0)
                        if rid == None:
                            print('line 174 error: rid cannot be None')
                        record = self.create_record(rid, rid, data[i], data, first)
                        meta_cols = record.get_meta()
                        # print(meta_cols)
                        prange = self.prange_directory.get(i)[-1]
                        record.offset = prange.b_page[-1].writeRecord(data[i])
                        record.page_pos = len(prange.b_page) - 1
                        record.prange_pos = self.prange_num
                    else:
                        prange = self.prange_directory.get(i)[-1]
                        prange.b_page[-1].writeRecord(data[i]) 
                    self.index.insert(i, data[i], record.rid)
                else:
                    # print('pos',i,record.prange_pos,record.page_pos,meta_cols[i - self.num_columns])
                    # print(meta_cols[0])
                    prange = self.prange_directory.get(i)[record.prange_pos]
                    prange.b_page[record.page_pos].writeRecord(meta_cols[i - self.num_columns])    
            else:
                if self.insert_page_to(i) == -1:
                    self.add_prange(i)
                    # self.free_trid = 0
                    if i < self.num_columns:
                        if first == True:
                            # print('adding prange')
                            self.prange_num += 1
                            rid = self.next_free_rid(0)
                            record = self.create_record(rid, rid, data[i], data, first)
                            meta_cols = record.get_meta()
                            prange = self.prange_directory.get(i)[-1]
                            record.offset = prange.b_page[-1].writeRecord(data[i])
                            record.page_pos = len(prange.b_page) - 1
                            record.prange_pos = self.prange_num
                        else:
                            prange = self.prange_directory.get(i)[-1]
                            prange.b_page[-1].writeRecord(data[i])
                        self.index.insert(i, data[i], record.rid)
                    else:
                        prange = self.prange_directory.get(i)[record.prange_pos]
                        prange.b_page[record.page_pos].writeRecord(meta_cols[i - self.num_columns])  
                    # self.free_brid = 0 
                else:
                    # self.free_trid = 0
                    if i < self.num_columns:
                        if first == True:
                            rid = self.next_free_rid(0)
                            record = self.create_record(rid, rid, data[i], data, first)
                            meta_cols = record.get_meta()
                            prange = self.prange_directory.get(i)[-1]
                            record.offset = prange.b_page[-1].writeRecord(data[i])
                            record.page_pos = len(prange.b_page) - 1
                            record.prange_pos = self.prange_num
                        else:
                            prange = self.prange_directory.get(i)[-1]
                            prange.b_page[-1].writeRecord(data[i])
                        self.index.insert(i, data[i], record.rid)
                    else:
                        prange = self.prange_directory.get(i)[record.prange_pos]
                        prange.b_page[record.page_pos].writeRecord(meta_cols[i - self.num_columns])  
        return True

    def checkSchema(self, record, data):
        schema = ''
        for i in range(self.num_columns):
            if record.columns[i] == data[i] or data[i] == None:
                schema += '1'
            else:
                schema += '2'
        # print('243. schema', schema)
        return int(schema)

    # if key does not exist then return false
    # To Do: update record to index
    def update_record(self, key, *data, delete):
        # print(data)
        schema = None
        if key == None:
            print('empty key')
        data = list(data)
        std_id = key
        rid = self.next_free_rid(1)
        # print(rid)
        base_record = self.page_directory.get(key)
        # if base_record.indirect == None:
        #     print('got none indirect', key)
        #     return 
        if base_record == None:
            print('record doesnt exist')
            return False
        # get current prange position
        # print(base_record.indirect)
        cur_prange_pos = base_record.prange_pos
        prev_record = self.page_directory.get(base_record.indirect)
        if prev_record.rid == None:
            print('None prev_record')
            return
        if delete == True:
            schema = int('1' * self.num_columns)
        else:
            schema = self.checkSchema(prev_record, data)
            data[0] = key
        cur_record = Record(rid, prev_record.indirect, key, data)
        cur_record.schema = schema
        meta_cols = cur_record.get_meta()
        # construct linked list
        base_record.indirect = cur_record.rid
        base_record.schema = schema
        brecord_meta = base_record.get_meta()

        self.prange_directory.get(self.num_columns + INDIRECTION_COLUMN)[base_record.prange_pos].b_page[base_record.page_pos].updateRecord(base_record.offset, brecord_meta[INDIRECTION_COLUMN])
        
        self.prange_directory.get(self.num_columns + SCHEMA_ENCODING_COLUMN)[base_record.prange_pos].b_page[base_record.page_pos].updateRecord(base_record.offset, schema)
        cur_record.indirect = prev_record.rid
        self.page_directory.update({cur_record.rid: cur_record})
        for i in range(self.num_columns + META_DATA_COL_NUM):
            prev_data = None
            if i < self.num_columns:
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
                    if delete == True or data_ == '/':
                        data_ = None
                    # print('176',prev_record.rid, prev_record.prange_pos,
                    # prev_record.page_pos, data_)
            if self.prange_directory.get(i)[cur_prange_pos].t_page[-1].has_capacity() == True:
                if i < self.num_columns:
                    prange = self.prange_directory.get(i)[cur_prange_pos]
                    # print(data_)
                    cur_record.columns[i] = data_
                    cur_record.offset = prange.t_page[-1].writeRecord(data_)
                    cur_record.page_pos = len(prange.t_page) - 1
                    cur_record.prange_pos = cur_prange_pos
                    self.index.update(i, prev_data, data_, base_record.rid)
                else:
                    # print('writing meta data', i , cur_prange_pos, cur_record.page_pos)
                    prange = self.prange_directory.get(i)[cur_prange_pos]
                    prange.t_page[cur_record.page_pos].writeRecord(meta_cols[i - self.num_columns])    
            else:
                if i < self.num_columns:
                    self.update_page_to(i, cur_prange_pos)
                    prange = self.prange_directory.get(i)[cur_prange_pos]
                    cur_record.columns[i] = data_
                    cur_record.offset = prange.t_page[-1].writeRecord(data_)
                    cur_record.page_pos = len(prange.t_page) - 1
                    cur_record.prange_pos = cur_prange_pos
                    self.index.update(i, prev_data, data_, base_record.rid)
                else:
                    self.update_page_to(i, cur_prange_pos)
                    # print('writing meta data')
                    prange = self.prange_directory.get(i)[cur_prange_pos]
                    prange.t_page[cur_record.page_pos].writeRecord(meta_cols[i - self.num_columns])
        if delete == True:
            return cur_record
        return 0

    def insert_page_to(self, ith_column):
        prange = self.prange_directory.get(ith_column)[-1]
        if prange.append_page(0) == -1:
            return -1  

    def get_rid_list(self):
        return self.rid_list

    def update_page_to(self, ith_column, ith_prange):
        prange = self.prange_directory.get(ith_column)[ith_prange]
        prange.append_page(1)

    def get_data(self, rid, ith_col, prange_pos, page_pos, offset):
        if rid[0] == 'b':
            page = self.prange_directory.get(ith_col)[prange_pos].b_page[page_pos]
            return page.readRecord(offset)
        page = self.prange_directory.get(ith_col)[prange_pos].t_page[page_pos]
        # print(page.readRecord(offset))
        return page.readRecord(offset)

    def read_record(self, key):
        record = self.page_directory.get(key)
        if record == None:
            print('The key doesnt exist or empty key')
        rid = record.indirect
        # print(rid)
        record = self.page_directory.get(rid)
        offset = record.offset
        page_pos = record.page_pos
        prange_pos = record.prange_pos
        # print('record info', page_pos, prange_pos, offset)
        res = []
        for i in range(self.num_columns + META_DATA_COL_NUM):
            if rid[0] == 'b':
                page = self.prange_directory.get(i)[prange_pos].b_page[page_pos]
                data = page.readRecord(offset)
                # print(data)
                res.append(data)
            else:
                # print(offset)
                page = self.prange_directory.get(i)[prange_pos].t_page[page_pos]
                data = page.readRecord(offset)
                # print(data)
                res.append(data)
        return res

    def __merge(self,*data):
        for i in range(self.num_columns + META_DATA_COL_NUM):
            self.load_basepage_copy_to_memory(i)
            self.sort_tailpage_reverse(i)
            

